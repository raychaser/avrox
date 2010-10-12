package org.digitalresearch.avrox.messaging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

import com.rabbitmq.client.*;
import org.slf4j.*;

/**
 * A Receiver that binds to the AMQP Advanced Message Queue Protocol.
 *
 * @author Christian (raychaser@gmail.com)
 */
public class AMQPReceiver implements Receiver, AMQPInboundTransport.Listener {

  // Log.

  protected static final Logger LOG = LoggerFactory.getLogger(AMQPReceiver.class);

  // Instance fields.

  protected final Consumer consumer;
  protected final String exchangeName;
  protected final String queueName;
  protected final String routingKey;

  protected final ConnectionFactory factory;

  // Implementation.

  public AMQPReceiver(Consumer consumer, String hostname, int port,
                      String username, String password, String virtualHost,
                      String exchangeName, String queueName, String routingKey) {
    this.consumer = consumer;
    this.exchangeName = exchangeName;
    this.queueName = queueName;
    this.routingKey = routingKey;

    // Create the AMQP connection factory.
    factory = createFactory(hostname, port, username, password, virtualHost);
  }

  protected ConnectionFactory createFactory(
      String hostname, int port, String username, String password, String virtualHost) {
    ConnectionFactory result = new ConnectionFactory();
    result.setUsername(username);
    result.setPassword(password);
    result.setVirtualHost(virtualHost);
    result.setRequestedHeartbeat(5); // TODO: Make configurable.
    result.setHost(hostname);
    result.setPort(port);
    return result;
  }

  // OneWayMessageConsumer implementation.

  @Override
  public void start() {

    // Start consuming from the queue.
    AMQPInboundTransport in =
        new AMQPInboundTransport(factory, exchangeName, routingKey, queueName, true);
    in.addListener(this);
    Thread consumerThread = new Thread(in);
    consumerThread.setName(
        String.format("AMQPReceiver-%s:%d-%s", factory.getHost(), factory.getPort(), queueName));
    consumerThread.setDaemon(true);
    consumerThread.start();
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
  }

  // AMQPConsumer.Listener implementation.

  @Override
  public void message(byte[] messageBytes) {
    List<ByteBuffer> buffers = new LinkedList<ByteBuffer>();
    buffers.add(ByteBuffer.wrap(messageBytes));
    try {
      consumer.consume(buffers);
    } catch (IOException ioe) {
      LOG.error("Error consuming AMQP message.", ioe);
    }
  }
}
