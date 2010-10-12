package org.digitalresearch.avrox.messaging;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * A Receiver binds the receiving exit point of a transport to a Consumer.
 * In other words, while a Consumer doesn't care about what underlying
 * transport is used to get messages, a Receiver does worry about that bridge.
 * Possible example transports include AMQP and HTTP.
 * <p/>
 * Producer -> Transmitter -> [...] -> Receiver -> Consumer
 *
 * @author Christian (raychaser@gmail.com)
 */
public interface Receiver {

  // Interface.

  void start();
  void dispatch(List<ByteBuffer> buffers);
  void stop();
}
