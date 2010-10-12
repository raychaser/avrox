package org.digitalresearch.avrox.messaging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * A Transmitter that uses a simple in-memory queue as a "transport". This
 * has it's uses mostly if not exclusively in test scenarios.
 *
 * @author Christian (raychaser@gmail.com)
 */
public class LocalTransmitter implements Transmitter {

  // Instance fields.

  protected final BlockingQueue<List<ByteBuffer>> queue;

  // Implementation.

  public LocalTransmitter(BlockingQueue<List<ByteBuffer>> queue) {
    this.queue = queue;
  }

  // Transmitter implementation.

  @Override
  public void start() {
  }

  public void writeBuffers(List<ByteBuffer> buffers) throws IOException {
    try {
      queue.put(buffers);
    } catch (InterruptedException ie) {
      ie.printStackTrace();
    }
  }

  @Override
  public void stop() {
  }
}
