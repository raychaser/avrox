package org.digitalresearch.avrox.messaging;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.avro.util.Utf8;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;

import org.digitalresearch.avrox.TestProtocol;
import org.digitalresearch.avrox.TestRecord;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.fail;

/**
 * @author Christian (raychaser@gmail.com)
 */
public class FileMessagingTest extends ZooKeeperSupport {

  // Implementation.

  public void basic(ProtocolRegistry protocolRegistry) throws IOException {

    // Create test file and stream.
    File file = new File("tmp");
    if (file.exists() && !file.delete()) throw new RuntimeException("Error deleting file");
    FileOutputStream out = new FileOutputStream(file);

    // Create the transmitter.
    OutputStreamTransmitter transmitter = new OutputStreamTransmitter(out);
    transmitter.start();

    // Transmit some data.
    long startTimestamp = System.currentTimeMillis();
    int recordCount = 1000;
    TestProtocol proxy = (TestProtocol)
        SpecificProducer.getClient(TestProtocol.class, protocolRegistry, transmitter);
    List<TestRecord> testRecords = new ArrayList<TestRecord>(recordCount);
    for (int i = 0; i < recordCount; i++) {
      TestRecord testRecord = new TestRecord();
      testRecord.testString = new Utf8("rub-a-dub-dub- " + i);
      testRecord.testInt = 666 + i;
      testRecords.add(testRecord);
      proxy.test(testRecord);
    }
    transmitter.close();
    System.out.printf("Done writing in '%d'\n", System.currentTimeMillis() - startTimestamp);

    // Create the receiver.
    startTimestamp = System.currentTimeMillis();
    FileInputStream in = new FileInputStream(file);
    TestProtocolHandler handler = new TestProtocolHandler();
    Consumer consumer = new SpecificConsumer(TestProtocol.class, protocolRegistry, handler);
    InputStreamReceiver receiver = new InputStreamReceiver(consumer, in, null);
    receiver.start();

    // Wait until everything has been received.
    while (System.currentTimeMillis() < startTimestamp + 5000 &&
        handler.getTestRecords().size() != recordCount) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException ie) {
        ie.printStackTrace();
      }
    }
    System.out.printf("Done reading in '%d'\n", System.currentTimeMillis() - startTimestamp);
    if (handler.getTestRecords().size() != recordCount)
      fail("Read loop timed out");

    // Make sure we got the right data.
    assertEquals(recordCount, handler.getTestRecords().size());
    for (int i = 0; i < recordCount; i++) {
      TestRecord src = testRecords.get(i);
      TestRecord dst = handler.getTestRecords().get(i);
      assertEquals(src.testString, dst.testString);
      assertEquals(src.testInt, dst.testInt);
    }
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

    public List<TestRecord> testRecords = new LinkedList<TestRecord>();

    // Implementation.

    public List<TestRecord> getTestRecords() {
      return testRecords;
    }

    // TestProtocol implementation.

    @Override
    public void test(TestRecord testRecord) {
      testRecords.add(testRecord);
    }
  }
}
