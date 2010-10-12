package org.digitalresearch.avrox.ghetto;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

import com.rabbitmq.client.*;

import org.digitalresearch.avrox.messaging.*;

/**
 * @author Christian (raychaser@gmail.com)
 */
public class AMQPOneWayMessageInboundTransport implements OneWayMessageInboundTransport, AMQPInboundTransport.Listener {

  // Instance fields.

  protected final String hostname;
  protected final int port;
  protected final String virtualHost;
  protected final String exchangeName;
  protected final String queueName;
  protected final String routingKey;

  protected final ConnectionFactory factory;

  protected final List<OneWayMessageProcessor> processors = new LinkedList<OneWayMessageProcessor>();

  protected AMQPInboundTransport inboundTransport;

  // Implementation.

  public AMQPOneWayMessageInboundTransport(String hostname, int port, String virtualHost,
                                           String exchangeName, String queueName, String routingKey) {
    this.hostname = hostname;
    this.port = port;
    this.virtualHost = virtualHost;
    this.exchangeName = exchangeName;
    this.queueName = queueName;
    this.routingKey = routingKey;

    // Create the AMQP connection factory.
    factory = new ConnectionFactory();
    factory.setUsername("guest");
    factory.setPassword("guest");
    factory.setVirtualHost(this.virtualHost);
    factory.setRequestedHeartbeat(5);
    factory.setHost(this.hostname);
    factory.setPort(this.port);
  }

  // OneWayMessageConsumer implementation.

  @Override
  public void addProcessor(OneWayMessageProcessor messageProcessor) {
    synchronized (processors) {
      processors.add(messageProcessor);
    }
  }

  @Override
  public void start() {
    // Start consumer
    inboundTransport = new AMQPInboundTransport(factory, exchangeName, routingKey, queueName, true);
    inboundTransport.addListener(this);
    Thread consumerThread = new Thread(inboundTransport, String.format(
        "AMQP-Consumer-%s:%d-%s-%s", hostname, port, exchangeName, queueName));
//    consumerThread.setDaemon(true);
    consumerThread.start();
  }

  // AMQPConsumer implementation.

  public void message(byte[] messageBytes) {
    List<ByteBuffer> buffers = new LinkedList<ByteBuffer>();
    buffers.add(ByteBuffer.wrap(messageBytes));
    synchronized (processors) {
      for (OneWayMessageProcessor processor : processors) {
        processor.received(buffers);
      }
    }
  }
}
