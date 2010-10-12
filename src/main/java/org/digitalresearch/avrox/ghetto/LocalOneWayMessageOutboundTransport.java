package org.digitalresearch.avrox.ghetto;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * @author Christian (raychaser@gmail.com)
 */
public class LocalOneWayMessageOutboundTransport implements OneWayMessageOutboundTransport {

  // Instance fields.

  protected final BlockingQueue<List<ByteBuffer>> queue;

  // Implementation.

  public LocalOneWayMessageOutboundTransport(BlockingQueue<List<ByteBuffer>> queue) {
    this.queue = queue;
  }

  // OneWayMessageProducer implementation.

  @Override
  public void publish(List<ByteBuffer> buffers) {
    try {
      queue.put(buffers);
    } catch (InterruptedException ie) {
      throw new RuntimeException(ie);
    }
  }
}
