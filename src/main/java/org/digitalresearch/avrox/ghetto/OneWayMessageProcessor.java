package org.digitalresearch.avrox.ghetto;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Simple callback interface used by OneWayMessageConsumer to notify
 * listeners of received messages.
 *
 * @author Christian (raychaser@gmail.com)
 */
public interface OneWayMessageProcessor {

  // Interface.

  /**
   * Handle and process the specified message.
   *
   * @param buffers The received message.
   */
  void received(List<ByteBuffer> buffers);
}
