package org.digitalresearch.avrox.messaging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * A Transmitter binds a Producer to the sending entry point of a transport.
 * A Producer doesn't care about what underlying transport is be used to
 * transmit messages to Consumers, but a Transmitter does worry about that
 * bridge. Possible example transports include AMQP and HTTP. Transmitters
 * will usually have a counterpart Receiver. While it is theoretically
 * possible that the Receiver has a binding to a different transport, this
 * is not currently assumed.
 * <p/>
 * Producer -> Transmitter -> [...] -> Receiver -> Consumer
 *
 * @author Christian (raychaser@gmail.com)
 */
public interface Transmitter {

  // Interface.

  void start();

  void writeBuffers(List<ByteBuffer> buffers) throws IOException;

  void stop();
}
