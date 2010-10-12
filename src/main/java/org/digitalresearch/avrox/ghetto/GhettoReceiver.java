package org.digitalresearch.avrox.ghetto;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.avro.ipc.*;

/**
 * Implementation of a receiver based on the existing IPC transceiver.
 * This is obviously very dirty and depends more or less on an
 * understanding of the internals of Transceiver, which in turn leads
 * to this being a very brittle proposition. Nonetheless, implementing
 * this has helped form an understanding of the details of the Transceiver.
 *
 * @author Christian (raychaser@gmail.com)
 */
public class GhettoReceiver extends Transceiver implements OneWayMessageProcessor {

  // Instance fields.

  protected final OneWayMessageInboundTransport in;
  protected final Responder responder;

  // Implementation.

  public GhettoReceiver(OneWayMessageInboundTransport in, Responder responder) {
    this.in = in;
    this.responder = responder;
    in.addProcessor(this);
    in.start();
  }

  // Transceiver implementation.

  @Override
  public boolean isConnected() {
    // TODO: No notion of a session (yet).
    return false;
  }

  @Override
  public String getRemoteName() {
    // TODO: What goes here?
    return "one-way-or-the-highway";
  }

  @Override
  public List<ByteBuffer> readBuffers() throws IOException {
    // TODO: Ugly.
    throw new UnsupportedOperationException("Not for Receiver");
  }

  @Override
  public void writeBuffers(List<ByteBuffer> buffers) throws IOException {
    // TODO: Ugly.
    throw new UnsupportedOperationException("Not for Receiver");
  }

  // OneWayMessageProcessor implementation.

  @Override
  public void received(List<ByteBuffer> buffers) {
    try {
      responder.respond(buffers, this);
    } catch (IOException ioe) {
      ioe.printStackTrace();
      // TODO: Is this reasonable?
      throw new RuntimeException(ioe);
    }
  }
}
