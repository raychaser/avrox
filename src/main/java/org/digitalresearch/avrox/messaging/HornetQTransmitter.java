package org.digitalresearch.avrox.messaging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hornetq.api.core.*;
import org.hornetq.api.core.client.*;
import org.hornetq.core.remoting.impl.netty.*;
import org.slf4j.*;

/**
 * @author Christian (raychaser@gmail.com)
 */
public class HornetQTransmitter implements Transmitter {

  // Log.

  protected static final Logger LOG = LoggerFactory.getLogger(HornetQTransmitter.class);

  // Instance fields.

  protected final String host;
  protected final int port;
  protected final String address;
  protected final boolean durable;
  protected final long expirationMillis;
  protected final String connectionString;

  protected ClientSessionFactory clientSessionFactory;
  protected ClientSession clientSession;
  protected ClientProducer clientProducer;

  // Implementation.

  public HornetQTransmitter(String host, int port, String address,
                            boolean durable, long expirationMillis) {
    this.host = host;
    this.port = port;
    this.address = address;
    this.durable = durable;
    this.expirationMillis = expirationMillis;
    this.connectionString = String.format("%s:%d@%s", host, port, address);
  }

  protected void connect() {

    // Make sure we are disconnected.
    disconnect();

    try {

      // Connect.
      clientSessionFactory = createClientSessionFactory();
      clientSession = createClientSession(clientSessionFactory);
      clientProducer = createClientProducer(clientSession);

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected void disconnect() {

    // Close the client producer.
    if (clientProducer != null) {
      try {
        LOG.info("Closing HornetQ client producer to: '{}'", connectionString);
        clientProducer.close();
      } catch (HornetQException hqe) {
        LOG.error("Error closing HornetQ client producer to: '" + connectionString + "'", hqe);
      }
    }

    // Close the client session.
    if (clientSession != null) {
      try {
        LOG.info("Closing HornetQ client session to: '{}'", connectionString);
        clientSession.close();
      } catch (HornetQException hqe) {
        LOG.error("Error closing HornetQ client session to: '" + connectionString + "'", hqe);
      }
    }

    // Close the client session factory.
    if (clientSessionFactory != null) {
      LOG.info("Closing HornetQ client producer to: '{}'", connectionString);
      clientSessionFactory.close();
    }
  }

  protected ClientSessionFactory createClientSessionFactory() {

    // Hash out the transport configuration.
    Map<String, Object> transportConfiguration = new HashMap<String, Object>();
    transportConfiguration.put("host", host);
    transportConfiguration.put("port", port);
    configureTransportConfiguration(transportConfiguration);
    LOG.info("HornetQ transport configuration: {}", transportConfiguration);

    // Create and configure the client session factory.
    ClientSessionFactory result = HornetQClient.createClientSessionFactory(
        new TransportConfiguration(getTransportClassName(), transportConfiguration));
    configureClientSessionFactory(result);
    return result;
  }

  protected void configureTransportConfiguration(Map<String, Object> transportConfiguration) {
    // Subclasses might want to configure the transport configuration.
  }


  protected String getTransportClassName() {
    return NettyConnectorFactory.class.getName();
  }

  protected void configureClientSessionFactory(ClientSessionFactory clientSessionFactory) {
    // Subclasses might want to configure the client session factory.
  }

  private ClientSession createClientSession(ClientSessionFactory factory)
      throws HornetQException {
    ClientSession result = factory.createSession();
    return result;
  }

  protected ClientProducer createClientProducer(ClientSession clientSession)
      throws HornetQException {
    ClientProducer result = clientSession.createProducer(address);
    return result;
  }

  protected void createAndSendMessage(byte[] bytes) throws HornetQException {
    ClientMessage message = clientSession.createMessage(durable);
    if (expirationMillis != -1) message.setExpiration(expirationMillis);
    message.getBodyBuffer().writeBytes(bytes);
    clientProducer.send(message);
  }

  // Transmitter implementation.

  @Override
  public void start() {

    // Connect.
    connect();

    // Shutdown hook just in case...
    Runtime.getRuntime().addShutdownHook(new Thread("Shutdown-HornetQTransmitter-" + this) {
      @Override
      public void run() {
        LOG.info("HornetQReceiver shutdown...");
        HornetQTransmitter.this.stop();
      }
    });
  }

  @Override
  public void writeBuffers(List<ByteBuffer> buffers) throws IOException {

    // Construct the byte array to send.
    int totalLength = 0;
    for (ByteBuffer buffer : buffers)
      totalLength += buffer.remaining();
    byte[] bytes = new byte[totalLength];
    int offset = 0;
    for (ByteBuffer buffer : buffers) {
      int length = buffer.remaining();
      buffer.get(bytes, offset, length);
      offset += length;
    }

    // Send the message, with possible retry.
    try {
      createAndSendMessage(bytes);
    } catch (HornetQException hqe) {
      throw new IOException(hqe);
    }
  }

  @Override
  public void stop() {
    disconnect();
  }
}
