package org.digitalresearch.avrox.messaging;

import com.rabbitmq.client.*;
import org.slf4j.*;

/**
 * Implementation of an AMQP sender. Pools connections per thread and includes
 * some reconnection logic. This can be useful for more generic message sending
 * applications and has therefore been broken out into its own class.
 *
 * @author Christian (raychaser@gmail.com)
 */
public class AMQPOutboundTransport {

  // Log.

  protected static final Logger LOG = LoggerFactory.getLogger(AMQPOutboundTransport.class);

  // Thread locals.

  protected final ThreadLocal<Channel> channel = new ThreadLocal<Channel>() {
    protected Connection connection;

    protected Channel initialValue() {
      try {
        LOG.info("Creating a new AMQP connection to '{}:{}', vhost: '{}'",
            new Object[]{factory.getHost(), factory.getPort(), factory.getVirtualHost()});
        connection = factory.newConnection();
        LOG.info("Creating a new AMQP channel to '{}:{}', vhost: '{}'",
            new Object[]{factory.getHost(), factory.getPort(), factory.getVirtualHost()});
        return connection.createChannel();
      } catch (Throwable t) {
        LOG.info("Exception while creating connection or channel to '{}', vhost: '{}'",
            new Object[]{factory.getHost(), factory.getPort(), factory.getVirtualHost()});
        connection = null;
        throw new RuntimeException(t);
      }
    }

    ;
  };

  // Instance fields.

  protected final ConnectionFactory factory;
  protected final int maxTries;
  protected final long sleepMillis;

  // Implementation.

  public AMQPOutboundTransport(ConnectionFactory factory, int maxTries, long sleepMillis) {
    this.factory = factory;
    this.maxTries = maxTries;
    this.sleepMillis = sleepMillis;
  }

  public Channel getChannel() {
    return channel.get();
  }

  public void publishBytes(String routingKey, String exchangeName, byte[] bytes) {
    int tries = 0;
    Throwable lastException = null;
    boolean success = false;
    while (tries++ < maxTries && !success) {
      try {

        // Publish the bytes.
        LOG.debug("Attempt: '{}', publishing to exchange '{}', routing key: '{}', " +
            "# of bytes: '{}', to '{}:{}', vhost: '{}'",
            new Object[]{tries - 1, exchangeName, routingKey, bytes.length,
                factory.getHost(), factory.getPort(), factory.getVirtualHost()});
        channel.get().basicPublish(
            exchangeName,
            routingKey,
            MessageProperties.PERSISTENT_BASIC,
            bytes);

        // We are done here.
        success = true;

      } catch (AlreadyClosedException ace) {
        LOG.warn("AMQP error on try '{}' of '{}': '{}' - sleeping for a bit",
            new Object[]{tries, maxTries, ace.getMessage()});
        lastException = ace;
        try {
          Thread.sleep(sleepMillis);
        } catch (InterruptedException ie) {
        }
        LOG.info("Channel is not open, forcing reconnect");
        channel.remove();
      } catch (Throwable t) {
        LOG.error("Error on try '{}' of '{}'- sleeping for a bit",
            new Object[]{tries, maxTries, t.getMessage()});
        LOG.error("Error", t);
        lastException = t;
        try {
          Thread.sleep(sleepMillis);
        } catch (InterruptedException ie) {
        }
        LOG.info("Channel is not open, forcing reconnect");
        channel.remove();
      }
    }

    // Throw a runtime exception if unsuccessful.
    if (!success) {
      LOG.error("No success after '{}' tries, last exception: '{}'",
          new Object[]{tries - 1, lastException});
      throw new RuntimeException(lastException);
    }
  }
}
