package org.digitalresearch.avrox.messaging;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.List;

/**
 * A Transmitter that uses an output stream as a message sink. Of course, the
 * output stream itself can bind to any number of other transports. Useful
 * for example for recording messages to a file.
 *
 * @author Christian (raychaser@gmail.com)
 */
public class OutputStreamTransmitter implements Transmitter {

  // Instance fields.

  protected final OutputStream out;
  protected final WritableByteChannel channel;

  // Implementation.

  public OutputStreamTransmitter(OutputStream out) {
    this.out = out;
    channel = Channels.newChannel(out);
  }

  public void close() throws IOException {
    writeInt(out, -1);
    out.close();
  }

  protected void writeInt(OutputStream out, int v) throws IOException {
    out.write((v >>> 24) & 0xFF);
    out.write((v >>> 16) & 0xFF);
    out.write((v >>> 8) & 0xFF);
    out.write((v >>> 0) & 0xFF);
  }

  // Transmitter implementation.


  @Override
  public void start() {
  }

  public void writeBuffers(List<ByteBuffer> buffers) throws IOException {
    synchronized (this) {
      int byteCount = 0;
      for (ByteBuffer buffer : buffers) byteCount += buffer.remaining();
      writeInt(out, byteCount);
      for (ByteBuffer buffer : buffers) channel.write(buffer);
      out.flush();
    }
  }

  @Override
  public void stop() {
  }
}
