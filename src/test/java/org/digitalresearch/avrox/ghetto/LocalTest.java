package org.digitalresearch.avrox.ghetto;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.avro.ipc.Responder;
import org.apache.avro.specific.SpecificRequestor;
import org.apache.avro.specific.SpecificResponder;
import org.apache.avro.util.Utf8;
import org.junit.Test;

import org.digitalresearch.avrox.TestProtocol;
import org.digitalresearch.avrox.TestRecord;

import static org.junit.Assert.assertEquals;

public class LocalTest {

  // Tests.

  @Test
  public void localGhettoTestProtocol() throws IOException {

    // Create the queue to be shared between the producer and the in.
    BlockingQueue<List<ByteBuffer>> queue = new LinkedBlockingQueue<List<ByteBuffer>>(100);

    // Create a transmitter on top of the local transport.
    OneWayMessageOutboundTransport out = new LocalOneWayMessageOutboundTransport(queue);
    GhettoTransmitter transmitter = new GhettoTransmitter(out);

    // Create a receiver on top of the local transport.
    TestProtocolHandler testProtocolHandler = new TestProtocolHandler();
    Responder responder = new SpecificResponder(TestProtocol.class, testProtocolHandler);
    OneWayMessageInboundTransport in = new LocalOneWayMessageInboundTransport(queue);
    GhettoReceiver receiver = new GhettoReceiver(in, responder);

    // Create and transmit a test record.
    TestProtocol proxy = (TestProtocol)
        SpecificRequestor.getClient(TestProtocol.class, transmitter);
    TestRecord testRecord = new TestRecord();
    testRecord.testString = new Utf8("rub-a-dub-dub");
    testRecord.testInt = 666;
    proxy.test(testRecord);

    // Wait for the in thread to consume the message.
    long startTimestamp = System.currentTimeMillis();
    while (System.currentTimeMillis() < startTimestamp + 5000 &&
        testProtocolHandler.testRecord == null) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException ie) {
        ie.printStackTrace();
      }
    }

    // Make sure the test record made it across ok.
    TestRecord that = testProtocolHandler.testRecord;
    assertEquals(testRecord.testString, that.testString);
    assertEquals(testRecord.testInt, that.testInt);
  }


  // Inner classes.

  public static class TestProtocolHandler implements TestProtocol {

    // Instance fields.

    public TestRecord testRecord;

    // TestProtocol implementation.

    @Override
    public void test(TestRecord testRecord) {
      this.testRecord = testRecord;
    }
  }
}

