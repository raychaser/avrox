package org.digitalresearch.avrox.messaging;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.rabbitmq.client.*;
import org.slf4j.*;

/**
 * Listens to incoming AMQP messages and hands to a given listener, which should
 * be an AMQPReceiver. This functionality might be useful for a more general case
 * in which different message handling is required and so is broken out into
 * its own class.
 *
 * @author Christian (raychaser@gmail.com)
 */
public class AMQPInboundTransport implements Runnable {

  // Log.

  protected static final Logger LOG = LoggerFactory.getLogger(AMQPInboundTransport.class);

  // Instance fields.

  protected final ConnectionFactory factory;
  protected final String exchangeName;
  protected final String routingKey;
  protected final String queueName;
  protected final boolean durable;

  protected final List<Listener> listeners = new ArrayList<Listener>();

  protected Connection connection;

  // Implementation.

  public AMQPInboundTransport(ConnectionFactory factory, String exchangeName, String routingKey,
                              String queueName, boolean durable) {
    this.factory = factory;
    this.exchangeName = exchangeName;
    this.routingKey = routingKey;
    this.queueName = queueName;
    this.durable = durable;
  }

  public synchronized void addListener(Listener listener) {
    listeners.add(listener);
  }

  protected void consume() throws IOException {

    // Create the channel.
    Channel channel = createChannel();

    // Create the consumer.
    boolean noAck = false; // TODO: Make configurable.
    QueueingConsumer consumer = new QueueingConsumer(channel);
    channel.basicConsume(queueName, noAck, consumer);

    LOG.info("Consuming from queue: '{}', exchange '{}', routing key: '{}",
        new Object[]{queueName, exchangeName, routingKey});
    while (true) {
      QueueingConsumer.Delivery delivery;
      try {
        delivery = consumer.nextDelivery();
        LOG.debug("Consumed '{}' bytes from queue: '{}', exchange '{}', routing key: '{}",
            new Object[]{delivery.getBody().length, queueName, exchangeName, routingKey});
      } catch (InterruptedException ie) {
        continue;
      }
      try {
        for (Listener listener : listeners)
          listener.message(delivery.getBody());
      }
      finally {
        channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
      }
    }
  }

  protected Channel createChannel() throws IOException {
    Channel channel = connection.createChannel();
    channel.queueDeclare(queueName, durable, false, false, null); // TODO: Addtl config.
    channel.queueBind(queueName, exchangeName, routingKey);
    channel.basicQos(0, 100, false); // TODO: Addtl config.
    return channel;
  }

  // Runnable implementation.

  @Override
  public void run() {
    while (true) {
      try {

        // Create the connection.
        connection = factory.newConnection();

        // Consume from the queue.
        consume();

      } catch (AlreadyClosedException ace) {
        LOG.warn("AMQP error: '{}' - sleeping for a bit", new Object[]{ace.getMessage()});
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ie) {
        }
      } catch (Throwable t) {
        LOG.error("AMQP error - sleeping for a bit", t);
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ie) {
        }
      }
    }
  }

  // Inner classes.

  public interface Listener {

    // Interface.

    void message(byte[] messageBytes);
  }
}
