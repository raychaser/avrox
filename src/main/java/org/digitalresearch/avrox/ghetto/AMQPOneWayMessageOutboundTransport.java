package org.digitalresearch.avrox.ghetto;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import com.rabbitmq.client.*;

import org.digitalresearch.avrox.messaging.*;

/**
 * @author Christian (raychaser@gmail.com)
 */
public class AMQPOneWayMessageOutboundTransport implements OneWayMessageOutboundTransport {

  // Instance fields.

  protected final String virtualHost;
  protected final String exchangeName;
  protected final String routingKey;
  protected final ConnectionFactory factory;

  protected AMQPOutboundTransport publisher;

  // Implementation.

  public AMQPOneWayMessageOutboundTransport(
      String hostname, int port, String virtualHost, String exchangeName, String routingKey) {
    this.virtualHost = virtualHost;
    this.exchangeName = exchangeName;
    this.routingKey = routingKey;
    try {

      // Create the AMQP connection factory.
      factory = new ConnectionFactory();
      factory.setUsername("guest");
      factory.setPassword("guest");
      factory.setVirtualHost(virtualHost);
      factory.setRequestedHeartbeat(5);
      factory.setHost(hostname);
      factory.setPort(port);

      // Create the publisher.
      publisher = new AMQPOutboundTransport(factory, 3, 1000);

      // Create the exchange.
      boolean durable = true;
      publisher.getChannel().exchangeDeclare(exchangeName, "direct", durable);

    } catch (IOException ioe) {
      ioe.printStackTrace();
      throw new RuntimeException(ioe);
    }
  }

  // OneWayMessageProducer implementation.

  @Override
  public void publish(List<ByteBuffer> buffers) {
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
    publisher.publishBytes(routingKey, exchangeName, bytes);

  }
}
