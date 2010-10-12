package org.digitalresearch.avrox.messaging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * A transmitter that intends to piggy-pack on an existing HTTPReceiver,
 * using HTTP responses to send messages back to the HTTPReceiver's
 * HTTPTransmitter, which in turn can then use an HTTPTransmitterReceiver
 * to receive the messages. Nobody said life was going to be easy :)
 *
 * @author Christian (raychaser@gmail.com)
 */
public class ReceiverTransmitter implements Transmitter {

  // Instance fields.

  protected final TransmitterPiggybackReceiver receiver;

  // Implementation.

  public ReceiverTransmitter(TransmitterPiggybackReceiver receiver) {
    this.receiver = receiver;
  }

  // Transmitter implementation.

  @Override
  public void start() {
  }

  @Override
  public void writeBuffers(List<ByteBuffer> buffers) throws IOException {
    receiver.queueResponseMessage(buffers);
  }

  @Override
  public void stop() {
  }
}
