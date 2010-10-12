package org.digitalresearch.avrox.ghetto;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * @author Christian (raychaser@gmail.com)
 */
public class LocalOneWayMessageInboundTransport implements OneWayMessageInboundTransport {

  // Instance fields.

  final protected BlockingQueue<List<ByteBuffer>> queue;

  protected final List<OneWayMessageProcessor> processors = new LinkedList<OneWayMessageProcessor>();

  // Implementation.

  public LocalOneWayMessageInboundTransport(BlockingQueue<List<ByteBuffer>> queue) {
    this.queue = queue;
  }

  // OneWayMessageConsumer implementation.

  @Override
  public void addProcessor(OneWayMessageProcessor messageProcessor) {
    synchronized (processors) {
      processors.add(messageProcessor);
    }
  }

  @Override
  public void start() {
    new Thread() {
      @Override
      public void run() {
        while (true) {
          try {
            List<ByteBuffer> buffers = queue.take();
            synchronized (processors) {
              for (OneWayMessageProcessor processor : processors)
                processor.received(buffers);
            }
          } catch (Throwable t) {
            t.printStackTrace();
          }
        }
      }
    }.start();
  }
}
