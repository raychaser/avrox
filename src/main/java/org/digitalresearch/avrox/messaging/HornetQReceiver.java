package org.digitalresearch.avrox.messaging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
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
public class HornetQReceiver implements Receiver {

  // Log.

  protected static final Logger LOG = LoggerFactory.getLogger(HornetQReceiver.class);

  // Instance fields.

  protected final Consumer consumer;
  protected final String host;
  protected final int port;
  protected final String queueName;
  protected final String connectionString;

  protected ClientSessionFactory clientSessionFactory;
  protected ClientSession clientSession;
  protected ClientConsumer clientConsumer;

  // Implementation.

  public HornetQReceiver(Consumer consumer, String host, int port, String queueName) {
    this.consumer = consumer;
    this.host = host;
    this.port = port;
    this.queueName = queueName;
    this.connectionString = String.format("%s:%d/%s", host, port, queueName);
  }

  protected void connect() throws HornetQException {

    // Make sure we are disconnected.
    disconnect();

    try {

      // Connect.
      clientSessionFactory = createClientSessionFactory();
      clientSession = createClientSession(clientSessionFactory);
      clientConsumer = createClientConsumer(clientSession, queueName);

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected void disconnect() {

    // Close the client consumer.
    if (clientConsumer != null) {
      try {
        LOG.info("Closing HornetQ client consumer to: '{}'", connectionString);
        clientConsumer.close();
      } catch (HornetQException hqe) {
        LOG.error("Error closing HornetQ client consumer to: '" + connectionString + "'", hqe);
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

  protected ClientSession createClientSession(ClientSessionFactory factory)
      throws HornetQException {
    ClientSession result = factory.createSession();
    return result;
  }

  protected ClientConsumer createClientConsumer(ClientSession session, String queue)
      throws HornetQException {
    ClientConsumer result = session.createConsumer(queue);
    return result;
  }

  // Receiver implementation.

  @Override
  public void start() {
    try {

      // First of all, connect.
      connect();

      // Shutdown hook just in case...
      Runtime.getRuntime().addShutdownHook(new Thread("Shutdown-HortnetQReceiver-" + this) {
        @Override
        public void run() {
          LOG.info("HornetQReceiver shutdown...");
          HornetQReceiver.this.stop();
        }
      });

      clientConsumer.setMessageHandler(new MessageHandler() {
        public void safeOnMessage(ClientMessage clientMessage) {
          boolean acknowledge = true;
          int readableBytes = -1;
          try {
            HornetQBuffer hqBuffer = clientMessage.getBodyBuffer();
            readableBytes = hqBuffer.readableBytes();
            ByteBuffer buffer = ByteBuffer.allocate(readableBytes);
            hqBuffer.readBytes(buffer);
            buffer.flip();
            List<ByteBuffer> buffers = new ArrayList<ByteBuffer>(1);
            buffers.add(buffer);
            try {
              consumer.consume(buffers);
            } catch (IOException ioe) {
              acknowledge = false;
              LOG.error("Error processing consumed HornetQ message from: '" +
                  connectionString + "'", ioe);
            }
          } catch (Throwable t) {
            LOG.error("Error during safeOnMessage with readable bytes: '" + readableBytes + "'", t);
            throw new RuntimeException(t);
          } finally {
            try {
              if (acknowledge) {
                clientMessage.acknowledge();
                clientSession.commit();
              } else {
                LOG.warn("Not acknowledging message: '" + clientMessage.getMessageID() + "'");
              }
            } catch (HornetQException hqe) {
              LOG.error("Error acknowledging message to: '" + connectionString + "'", hqe);
            }
          }
        }

        @Override
        public void onMessage(ClientMessage clientMessage) {
          try {

            // Actually consume the message.
            safeOnMessage(clientMessage);

          } catch (Throwable t) {
            // Outer catch all because uncaught exceptions will kill the client session,
            // apparently (which is not good, but has been observed).
            LOG.error("Throwable caught in main message handler to: '" + connectionString + "'", t);
          }
        }
      });
      clientSession.start();
    } catch (HornetQException hqe) {
      throw new RuntimeException(hqe);
    }
  }

  @Override
  public void dispatch(List<ByteBuffer> buffers) {
    try {
      consumer.consume(buffers);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public void stop() {
    disconnect();
  }
}
