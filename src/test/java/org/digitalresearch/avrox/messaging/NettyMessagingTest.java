package org.digitalresearch.avrox.messaging;

import java.net.URI;

import org.apache.avro.util.*;
import org.junit.*;

import org.digitalresearch.avrox.*;

import static org.junit.Assert.assertEquals;

/**
 * @author Christian (raychaser@gmail.com)
 */
public class NettyMessagingTest extends ZooKeeperSupport {

  // Implementation.

  protected void basic(ProtocolRegistry protocolRegistry, boolean compress,
                       boolean receiverNio, boolean transmitterNio, boolean http) throws Exception {

    System.out.printf("\nTesting with compress: '%s', receiverNio: '%s', transmitterNio: '%s', http: '%s'\n\n",
        compress, receiverNio, transmitterNio, http);

    // Create a Netty receiver.
    TestProtocolHandler testProtocolHandler = new TestProtocolHandler();
    Consumer consumer = new SpecificConsumer(
        TestProtocol.class, protocolRegistry, testProtocolHandler);
    NettyReceiver receiver = new NettyReceiver(
        consumer, "localhost", 23663,
        compress, false, receiverNio, http,
        null, null, null, null);
    receiver.start();

    // Create an Netty transmitter.
    NettyTransmitter transmitter = new NettyTransmitter(
        "localhost", 23663, true,
        compress, false, transmitterNio, http,
        null, null, null, null);
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
//    basic(new LocalProtocolRegistry(), false, false, false);
//    basic(new LocalProtocolRegistry(), false, false, true);
//    basic(new LocalProtocolRegistry(), false, true, false);
//    basic(new LocalProtocolRegistry(), false, true, true);
    basic(new LocalProtocolRegistry(), false, true, false, false);
    basic(new LocalProtocolRegistry(), false, true, false, true);
    basic(new LocalProtocolRegistry(), false, true, true, false);
    basic(new LocalProtocolRegistry(), false, true, true, true);
    basic(new LocalProtocolRegistry(), true, true, false, false);
    basic(new LocalProtocolRegistry(), true, true, false, true);
    basic(new LocalProtocolRegistry(), true, true, true, false);
    basic(new LocalProtocolRegistry(), true, true, true, true);
  }

  @Test
  public void testZooKeeperProtocolRegistry() {
    try {
      createZooKeeper();
//      basic(new ZooKeeperProtocolRegistry("localhost:22181", "/avro/test/protocol"), false, false, false);
//      basic(new ZooKeeperProtocolRegistry("localhost:22181", "/avro/test/protocol"), false, false, true);
//      basic(new ZooKeeperProtocolRegistry("localhost:22181", "/avro/test/protocol"), false, true, false);
//      basic(new ZooKeeperProtocolRegistry("localhost:22181", "/avro/test/protocol"), false, true, true);
      basic(new ZooKeeperProtocolRegistry("localhost:22181", "/avro/test/protocol"), false, true, false, false);
      basic(new ZooKeeperProtocolRegistry("localhost:22181", "/avro/test/protocol"), false, true, false, true);
      basic(new ZooKeeperProtocolRegistry("localhost:22181", "/avro/test/protocol"), false, true, true, false);
      basic(new ZooKeeperProtocolRegistry("localhost:22181", "/avro/test/protocol"), false, true, true, true);
      basic(new ZooKeeperProtocolRegistry("localhost:22181", "/avro/test/protocol"), true, true, false, false);
      basic(new ZooKeeperProtocolRegistry("localhost:22181", "/avro/test/protocol"), true, true, false, true);
      basic(new ZooKeeperProtocolRegistry("localhost:22181", "/avro/test/protocol"), true, true, true, false);
      basic(new ZooKeeperProtocolRegistry("localhost:22181", "/avro/test/protocol"), true, true, true, true);
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

