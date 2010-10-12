package org.digitalresearch.avrox.messaging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.avro.util.Utf8;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;

import org.digitalresearch.avrox.TestProtocol;
import org.digitalresearch.avrox.TestRecord;

import static org.junit.Assert.assertEquals;

/**
 * @author Christian (raychaser@gmail.com)
 */
public class LocalMessagingTest extends ZooKeeperSupport {

  // Implementation.

  protected void basic(ProtocolRegistry protocolRegistry) throws IOException {

    // Create the queue to be shared between the producer and the consumer.
    BlockingQueue<List<ByteBuffer>> queue = new LinkedBlockingQueue<List<ByteBuffer>>(100);

    // Create a local transmitter.
    LocalTransmitter transmitter = new LocalTransmitter(queue);
    transmitter.start();

    // Create a local receiver.
    TestProtocolHandler testProtocolHandler = new TestProtocolHandler();
    Consumer consumer = new SpecificConsumer(
        TestProtocol.class, protocolRegistry, testProtocolHandler);
    LocalReceiver receiver = new LocalReceiver(queue, consumer);
    receiver.start();

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
  }

  // Tests.

  @Test
  public void testLocalProtocolRegistry() throws IOException {
    basic(new LocalProtocolRegistry());
  }

  @Test
  public void testZooKeeperProtocolRegistry() {
    try {
      createZooKeeper();
      basic(new ZooKeeperProtocolRegistry("localhost:22181", "/avro/test/protocol"));
    } catch (IOException ioe) {
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

