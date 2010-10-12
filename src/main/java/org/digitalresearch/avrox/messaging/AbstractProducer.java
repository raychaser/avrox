package org.digitalresearch.avrox.messaging;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.avro.*;
import org.apache.avro.io.*;
import org.apache.avro.ipc.*;
import org.apache.avro.util.*;
import org.slf4j.*;

/**
 * Base class for all producers. A producer knows how to serialize a message
 * to bytes and then hands the bytes off to a transmitter. A Producers
 * counterpart on the receiving end is a Consumer.
 * <p/>
 * Producer -> Transmitter -> [...] -> Receiver -> Consumer
 *
 * @author Christian (raychaser@gmail.com)
 */
public abstract class AbstractProducer implements Producer {

  // Log.

  protected static final Logger LOG = LoggerFactory.getLogger(AbstractProducer.class);

  // Constants.

  public static final Utf8 PROTOCOL_KEY = new Utf8("protocol");
  protected static final int WIRE_FORMAT_VERSION = 1;

  // Instance fields.

  protected final Protocol protocol;
  protected final ProtocolRegistry protocolRegistry;
  protected final Transmitter transmitter;

  protected final MD5 protocolMD5;

  protected final List<MessagingPlugin> messagingPlugins =
      Collections.synchronizedList(new ArrayList<MessagingPlugin>());

  protected boolean protocolAddedToMetaData;

  // Implementation.

  public AbstractProducer(Protocol protocol, ProtocolRegistry protocolRegistry, Transmitter transmitter) {
    this.protocol = protocol;
    this.protocolRegistry = protocolRegistry;
    this.transmitter = transmitter;

    // Make the protocol known to the registry.
    protocolRegistry.register(protocol);

    // Cache the protocol MD5.
    protocolMD5 = new MD5();
    protocolMD5.bytes(protocol.getMD5());
  }

  protected synchronized void addProtocolMetadata(Protocol protocol, Map<Utf8, ByteBuffer> meta) {
    if (!protocolAddedToMetaData) {
      try {
        byte[] protocolBytes = protocol.toString().getBytes("UTF8");
        meta.put(PROTOCOL_KEY, ByteBuffer.wrap(protocolBytes));
        protocolAddedToMetaData = true;
        LOG.info("Added protocol to meta data");
      } catch (UnsupportedEncodingException uee) {
        throw new RuntimeException(uee);
      }
    }
  }

  // Producer implementation.

  /**
   * Adds a new plugin to manipulate messaging meta data. Plugins are executed
   * in the order that they are added.
   *
   * @param plugin A plugin that will manipulate messaging meta data.
   */
  @Override
  public void addMessagingPlugin(MessagingPlugin plugin) {
    messagingPlugins.add(plugin);
  }

  @Override
  public void produce(String messageName, Object request) throws Exception {

    // Prepare buffers, encoder.
    ByteBufferOutputStream bbo = new ByteBufferOutputStream();
    Encoder out = new BinaryEncoder(bbo);

    // Get the message by name.
    MessagingContext context = new MessagingContext();
    Protocol.Message message = protocol.getMessages().get(messageName);
    if (message == null) throw new AvroRuntimeException(String.format(
        "%s is not a known message", messageName));
    if (!message.isOneWay()) throw new AvroRuntimeException(String.format(
        "%s is not a one-way message", messageName));
    context.setMessage(message);

    // Write the actual message request and get it back as the payload so we
    // can add it to the messaging context for plugins to have some fun.
    writeRequest(message.getRequest(), request, out);
    List<ByteBuffer> payload = bbo.getBufferList();
    context.setMessagePayload(payload);

    // Get meta data from plugins - this uses the previously written payload.
    context.setMessagePayload(payload);
    for (MessagingPlugin plugin : messagingPlugins) plugin.messageSending(context);

    // If this is the first time we are producing a message, add the
    // protocol as meta data.
    addProtocolMetadata(protocol, context.getMessageMeta());

    // Now write the full request.
    out.writeInt(WIRE_FORMAT_VERSION);
    Meta.META_WRITER.get().write(context.getMessageMeta(), out);
    out.writeFixed(protocolMD5.bytes());
    out.writeString(message.getName());
    bbo.append(payload);

    // Now that we have everything written in memory, pass it to the transmitter.
    List<ByteBuffer> requestBytes = bbo.getBufferList();
    transmitter.writeBuffers(requestBytes);
  }
}
