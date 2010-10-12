package org.digitalresearch.avrox.messaging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.avro.*;
import org.apache.avro.io.*;
import org.apache.avro.ipc.*;
import org.apache.avro.util.*;
import org.slf4j.*;

/**
 * Base class for all consumer implementations. A consumer receives messages
 * as a bunch of bytes, and then uses Avro to parse the bytes back into
 * messages which can then be dispatched. A Consumer's counterpart on the
 * sending end is a Producer.
 * <p/>
 * Producer -> Transmitter -> [...] -> Receiver -> Consumer
 *
 * @author Christian (raychaser@gmail.com)
 */
public abstract class AbstractConsumer implements Consumer {

  // Log.

  protected static final Logger LOG = LoggerFactory.getLogger(AbstractConsumer.class);

  // Constants.

  public static final Utf8 PROTOCOL_KEY = AbstractProducer.PROTOCOL_KEY;
  protected static final int WIRE_FORMAT_VERSION = 1;

  // Instance fields.

  protected final Protocol protocol;
  protected final ProtocolRegistry protocolRegistry;

  protected final List<MessagingPlugin> messagingPlugins =
      Collections.synchronizedList(new ArrayList<MessagingPlugin>());

  // Implementation.

  public AbstractConsumer(Protocol protocol, ProtocolRegistry protocolRegistry) {
    this.protocol = protocol;
    this.protocolRegistry = protocolRegistry;

    // Make the protocol known to the registry.
    protocolRegistry.register(protocol);
  }

  // Consumer implementation.

  /**
   * Adds a new plugin to manipulate messaging meta data. Plugins are executed
   * in the order that they are added.
   *
   * @param plugin A plugin that will manipulate messaging meta data.
   */
  public void addMessagingPlugin(MessagingPlugin plugin) {
    messagingPlugins.add(plugin);
  }

  public void consume(List<ByteBuffer> buffers) throws IOException {

    // Prepare decoder.
    Decoder in = DecoderFactory.defaultFactory().createBinaryDecoder(
        new ByteBufferInputStream(buffers), null);

    // Read the wire format version.
    int wireFormatVersion = in.readInt();
    if (wireFormatVersion != WIRE_FORMAT_VERSION)
      throw new IOException(String.format("Wrong wire format version. " +
          "Expected '%d', got '%d'", WIRE_FORMAT_VERSION, wireFormatVersion));

    // Read the meta data.
    MessagingContext context = new MessagingContext();
    context.setMessageMeta(Meta.META_READER.get().read(null, in));

    // Let's see if we have a protocol in the meta data.
    ByteBuffer protocolBuffer = context.getMessageMeta().get(PROTOCOL_KEY);
    if (protocolBuffer != null) {
      byte[] protocolBytes = new byte[protocolBuffer.remaining()];
      protocolBuffer.get(protocolBytes, 0, protocolBytes.length);
      String protocolString = new String(protocolBytes, "UTF8");
      Protocol protocol = Protocol.parse(protocolString);
      protocolRegistry.register(protocol);
    }

    // Read the producer's protocol MD5.
    byte[] md5Bytes = new byte[16];
    in.readFixed(md5Bytes);
    MD5 md5 = new MD5();
    md5.bytes(md5Bytes);
    Protocol remoteProtocol = protocolRegistry.get(md5);

    // Check if we have a protocol to match.
    if (remoteProtocol == null) {
      throw new IOException("No matching protocol for remote MD5");
    }

    // Figure out which protocol to use.
    Protocol actualProtocol = remoteProtocol;
    // TODO: Maybe this is a bit trivial.

    // Read the message name.
    String messageName = in.readString(null).toString();
    Protocol.Message message = actualProtocol.getMessages().get(messageName);
    if (message == null)
      throw new IOException("No such remote message: " + messageName);

    // Execute the plugins.
    context.setMessagePayload(buffers);
    context.setMessage(message);
    for (MessagingPlugin plugin : messagingPlugins) {
      plugin.messageReceiving(context);
    }

    try {

      // Read the request and dispatch the message.
      Object request = readRequest(message.getRequest(), in);
      consume(message, request);
      
    } catch (Throwable t) {
      throw new IOException(t);
    }
  }
}
