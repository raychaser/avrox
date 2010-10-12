package org.digitalresearch.avrox.ghetto;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.*;
import org.apache.avro.generic.*;
import org.apache.avro.io.*;
import org.apache.avro.ipc.*;
import org.apache.avro.specific.*;
import org.apache.avro.util.*;

/**
 * Implementation of a transmitter based on the existing IPC transceiver.
 * This is obviously very dirty and depends more or less on an
 * understanding of the internals of Transceiver, which in turn leads
 * to this being a very brittle proposition. Nonetheless, implementing
 * this has helped form an understanding of the details of the Transceiver.
 *
 * @author Christian (raychaser@gmail.com)
 */
public class GhettoTransmitter extends Transceiver {

  // Static fields.

  private static final Schema META =
      Schema.createMap(Schema.create(Schema.Type.BYTES));
  private static final GenericDatumWriter<Map<Utf8, ByteBuffer>> META_WRITER =
      new GenericDatumWriter<Map<Utf8, ByteBuffer>>(META);

  // Instance fields.

  protected final OneWayMessageOutboundTransport out;

  // Implementation.

  public GhettoTransmitter(OneWayMessageOutboundTransport out) {
    this.out = out;
  }

  // Transceiver implementation.

  @Override
  public String getRemoteName() {
    // TODO: Is this reasonable?
    return "one-way-or-the-highway";
  }

  @Override
  public List<ByteBuffer> readBuffers() throws IOException {
    // TODO: Not very elegant, produces a bunch of stuff that will be thrown away.
    // We are faking a response here because in reality there is no actual
    // response in one-way messaging, but yet, trying to bypass the handshake
    // business is pretty much over my head right now.
    HandshakeResponse response = new HandshakeResponse();
    response.match = HandshakeMatch.BOTH;
    ByteBufferOutputStream bbo = new ByteBufferOutputStream();
    Encoder out = new BinaryEncoder(bbo);
    SpecificDatumWriter<HandshakeResponse> handshakeWriter =
        new SpecificDatumWriter<HandshakeResponse>(HandshakeResponse.class);
    handshakeWriter.write(response, out);
    META_WRITER.write(new HashMap<Utf8, ByteBuffer>(), out);
    out.writeBoolean(false);
    return bbo.getBufferList();
  }

  @Override
  public void writeBuffers(List<ByteBuffer> buffers) throws IOException {
    out.publish(buffers);
  }
}
