package org.digitalresearch.avrox.messaging;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.List;

/**
 * A Receiver that uses an input stream as its source of messages. See also
 * OutputStreamReceiver. Both classes can together be used to serialize
 * message to files, for example, and then read them back. This is very
 * useful for unit testing and general stubbing out of runtime dependencies.
 *
 * @author Christian (raychaser@gmail.com)
 */
public class InputStreamReceiver implements Receiver {

  // Instance fields.

  protected final Consumer consumer;
  protected final InputStream in;
  protected final EOFListener eofListener;
  protected final ReadableByteChannel channel;

  // Implementation.

  public InputStreamReceiver(Consumer consumer, InputStream in, EOFListener eofListener) {
    this.consumer = consumer;
    this.in = in;
    this.eofListener = eofListener;
    channel = Channels.newChannel(in);
  }

  protected int readInt(InputStream in) throws IOException {
    int ch1 = in.read();
    int ch2 = in.read();
    int ch3 = in.read();
    int ch4 = in.read();
    if ((ch1 | ch2 | ch3 | ch4) < 0)
      throw new EOFException();
    return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
  }

  // Receiver implementation.

  @Override
  public void start() {
    try {
      while (true) {
        int byteCount = -1;
        try {
          byteCount = readInt(in);
        } catch (EOFException eofe) {
          byteCount = -1;
          if (eofListener != null) eofListener.onEOF();
        }
        if (byteCount == -1) break;
        ByteBuffer buffer = ByteBuffer.allocate(byteCount);
        channel.read(buffer);
        buffer.flip();
        List<ByteBuffer> buffers = new ArrayList<ByteBuffer>(1);
        buffers.add(buffer);
        consumer.consume(buffers);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
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
  }

  // Inner classes.

  public interface EOFListener {

    // Interface.

    void onEOF();
  }
}
