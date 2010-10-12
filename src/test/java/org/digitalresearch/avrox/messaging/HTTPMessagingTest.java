package org.digitalresearch.avrox.messaging;

import java.net.URI;

import org.apache.avro.util.*;
import org.apache.zookeeper.*;
import org.junit.*;

import org.digitalresearch.avrox.*;

import static org.junit.Assert.*;

/**
 * @author Christian (raychaser@gmail.com)
 */
public class HTTPMessagingTest extends ZooKeeperSupport {

  // Implementation.

  protected void basic(ProtocolRegistry protocolRegistry) throws Exception {

    // Create a local receiver.
    TestProtocolHandler testProtocolHandler = new TestProtocolHandler();
    Consumer consumer = new SpecificConsumer(
        TestProtocol.class, protocolRegistry, testProtocolHandler);
    HTTPReceiver receiver = new HTTPReceiver(consumer, "localhost", 23663, true, false);
    receiver.start();

    // Create an HTTP transmitter.
    HTTPTransmitter transmitter = new HTTPTransmitter(new URI("http://localhost:23663"), true);
    transmitter.start();

    try {

      // Create and transmit a test record.
      TestProtocol proxy = (TestProtocol)
          SpecificProducer.getClient(TestProtocol.class, protocolRegistry, transmitter);
      TestRecord testRecord = new TestRecord();
      testRecord.testString = new Utf8("rub-a-dub-dub");
      testRecord.testInt = 666;
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

    } finally {
      transmitter.stop();
      receiver.stop();
    }
  }

  // Tests.

  @Test
  public void testLocalProtocolRegistry() throws Exception {
    basic(new LocalProtocolRegistry());
  }

  @Test
  public void testZooKeeperProtocolRegistry() {
    try {
      createZooKeeper();
      basic(new ZooKeeperProtocolRegistry("localhost:22181", "/avro/test/protocol"));
    } catch (Exception ioe) {
      throw new RuntimeException(ioe);
    }
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
