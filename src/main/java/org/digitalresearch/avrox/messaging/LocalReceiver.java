package org.digitalresearch.avrox.messaging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * This Receiver can share an in-memory queue with a LocalTransmitter,
 * which can be useful for testing.
 *
 * @author Christian (raychaser@gmail.com)
 */
public class LocalReceiver implements Receiver {

  // Instance fields.

  protected final BlockingQueue<List<ByteBuffer>> queue;
  protected final Consumer consumer;

  protected boolean stopped;

  // Implementation.

  public LocalReceiver(BlockingQueue<List<ByteBuffer>> queue, Consumer consumer) {
    this.queue = queue;
    this.consumer = consumer;
  }

  // Receiver implementation.

  public void start() {
    new Thread() {
      @Override
      public void run() {
        while (!stopped) {
          try {
            List<ByteBuffer> buffers = queue.take();
            consumer.consume(buffers);
          } catch (Throwable t) {
            t.printStackTrace();
          }
        }
      }
    }.start();
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
    stopped = true;
  }
}
