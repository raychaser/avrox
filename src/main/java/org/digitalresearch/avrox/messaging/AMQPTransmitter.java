package org.digitalresearch.avrox.messaging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import com.rabbitmq.client.*;

/**
 * A Transmitter that binds to the AMQP Advanced Message Queue Protocol.
 * Obviously useful for sending messages down some message queue, managed
 * by for example RabbitMQ.
 *
 * @author Christian (raychaser@gmail.com)
 */
public class AMQPTransmitter implements Transmitter {

  // Instance fields.

  protected final String exchangeName;
  protected final String routingKey;
  protected final ConnectionFactory factory;
  protected final AMQPOutboundTransport out;

  // Implementation.

  public AMQPTransmitter(String hostname, int port, String username, String password,
                         String virtualHost, String exchangeName, String routingKey) {
    this.exchangeName = exchangeName;
    this.routingKey = routingKey;

    // Create the AMQP connection factory.
    factory = createFactory(hostname, port, username, password, virtualHost);

    // Create the publisher.
    out = new AMQPOutboundTransport(factory, 3, 1000);
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

  protected boolean getDurable() {
    return true; // TODO: Make configurable.
  }

  // Transmitter implementation.


  @Override
  public void start() {
    try {

      // Create the exchange.
      boolean durable = getDurable();
      out.getChannel().exchangeDeclare(exchangeName, "direct", durable);

    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  public void writeBuffers(List<ByteBuffer> buffers) throws IOException {
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
    out.publishBytes(routingKey, exchangeName, bytes);
  }

  @Override
  public void stop() {
  }
}
