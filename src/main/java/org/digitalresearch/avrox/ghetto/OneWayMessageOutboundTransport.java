package org.digitalresearch.avrox.ghetto;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Interface for implementations of the transmitting end of a one-way
 * transport, such as a message queue. This is a sister interface to
 * OneWayMessageConsumer, of course. The producer/consumer terminology
 * is used here to distinguish from the higher level Transmitter and
 * Receiver abstractions.
 *
 * @author Christian (raychaser@gmail.com)
 */
public interface OneWayMessageOutboundTransport {

  // Interface.

  /**
   * Publish the the the specified buffers on the desired transport.
   *
   * @param buffers The buffers to publish.
   */
  void publish(List<ByteBuffer> buffers);
}
