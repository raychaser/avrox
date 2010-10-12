package org.digitalresearch.avrox.messaging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Counterpart to the HTTPReceiverTransmitter. Receives messages that are
 * sent piggy-back via HTTP responses to an HTTPTransmitter.
 *
 * @author Christian (raychaser@gmail.com)
 */
public class TransmitterReceiver implements Receiver {

  // Instance fields.

  protected final Consumer consumer;
  protected final ReceiverPiggybackTransmitter transmitter;

  // Implementation.

  public TransmitterReceiver(Consumer consumer, ReceiverPiggybackTransmitter transmitter) {
    this.consumer = consumer;
    this.transmitter = transmitter;
    transmitter.addResponseMessageReceiver(this);
  }

  // Receiver implementation.

  @Override
  public void start() {
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
}
