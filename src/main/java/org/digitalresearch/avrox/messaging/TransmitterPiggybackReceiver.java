package org.digitalresearch.avrox.messaging;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * A receiver that can act as a transmitter.
 *
 * @author Christian (raychaser@gmail.com)
 */
public interface TransmitterPiggybackReceiver extends Receiver {

  // Interface.

  void queueResponseMessage(List<ByteBuffer> buffers);
}
