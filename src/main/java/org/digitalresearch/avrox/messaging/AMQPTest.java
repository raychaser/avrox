package org.digitalresearch.avrox.messaging;

import java.io.IOException;

import org.apache.avro.util.*;
import org.junit.*;

import org.digitalresearch.avrox.*;

import static org.junit.Assert.*;

/**
 * This test is not in the src/test/java hierarchy because it requires
 * a bunch of setup (namely, a running AMQP server such as RabbitMQ)
 * which isn't present in the unit test environment (ie, Hudson).
 *
 * @author Christian (raychaser@gmail.com)
 */
public class AMQPTest {

  // Tests.

  @Test
  public void amqpTestProtocol() throws IOException {

    // Create a protocol registry.
    ProtocolRegistry protocolRegistry = new LocalProtocolRegistry();

    // Create an AMQP transmitter.
    AMQPTransmitter transmitter = new AMQPTransmitter(
        "localhost", 5672, "guest", "guest", "/", "avro-test-protocol", "avro-test-protocol");

    // Create a receiver on top of the local transport.
    TestProtocolHandler testProtocolHandler = new TestProtocolHandler();
    Consumer consumer = new SpecificConsumer(
        TestProtocol.class, protocolRegistry, testProtocolHandler);
    AMQPReceiver receiver = new AMQPReceiver(consumer, "localhost", 5672, "guest", "guest", "/",
        "avro-test-protocol", "avro-test-protocol-queue", "avro-test-protocol");
    receiver.start();

    // Create and transmit a test record.
    TestProtocol proxy = (TestProtocol)
        SpecificProducer.getClient(TestProtocol.class, protocolRegistry, transmitter);
    TestRecord testRecord = new TestRecord();
    testRecord.testString = new Utf8("rub-a-dub-a-dub-dub");
    testRecord.testInt = 666;
    proxy.test(testRecord);
    testRecord = new TestRecord();
    testRecord.testString = new Utf8("club-a-club-a-love-a-love");
    testRecord.testInt = 23;
    proxy.test(testRecord);

    // Wait for the consumer thread to consume the message.
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

  // Static implementation.

  public static void main(String[] args) throws Exception {
    new AMQPTest().amqpTestProtocol();
  }
}
