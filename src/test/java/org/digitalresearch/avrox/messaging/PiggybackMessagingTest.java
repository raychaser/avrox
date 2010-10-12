package org.digitalresearch.avrox.messaging;

import java.net.URI;

import org.apache.avro.util.*;
import org.junit.*;

import org.digitalresearch.avrox.*;

import static org.junit.Assert.*;

/**
 * @author Christian (raychaser@gmail.com)
 */
public class PiggybackMessagingTest extends ZooKeeperSupport {

  // Implementation.

  protected void basic(ProtocolRegistry protocolRegistry,
                       ReceiverPiggybackTransmitterFactory transmitterFactory,
                       TransmitterPiggybackReceiverFactory receiverFactory) throws Exception {

    // Create the regular HTTP transmitter/receiver combo.
    ReceiverPiggybackTransmitter transmitter = transmitterFactory.create();
    TestProtocolHandler testProtocolHandler = new TestProtocolHandler();
    Consumer consumer = new SpecificConsumer(
        TestProtocol.class, protocolRegistry, testProtocolHandler);
    TransmitterPiggybackReceiver receiver = receiverFactory.create(consumer);
    receiver.start();
    transmitter.start();

    // Let's now create the piggy-back transmitter/receiver pair.
    ReceiverTransmitter piggybackTransmitter = new ReceiverTransmitter(receiver);
    TestProtocolHandler piggyBackTestProtocolHandler = new TestProtocolHandler();
    Consumer piggyBackConsumer = new SpecificConsumer(
        TestProtocol.class, protocolRegistry, piggyBackTestProtocolHandler);
    TransmitterReceiver piggybackReceiver =
        new TransmitterReceiver(piggyBackConsumer, transmitter);
    piggybackReceiver.start();
    piggybackTransmitter.start();

    try {

      // Create and transmit a test record.
      TestProtocol proxy = (TestProtocol)
          SpecificProducer.getClient(TestProtocol.class, protocolRegistry, transmitter);
      TestRecord testRecord = new TestRecord();
      testRecord.testString = new Utf8("rub-a-dub-dub");
      testRecord.testInt = 666;
      proxy.test(testRecord);

      // Create and transmit a test record via the piggy back route.
      TestProtocol piggyBackProxy = (TestProtocol)
          SpecificProducer.getClient(TestProtocol.class, protocolRegistry, piggybackTransmitter);
      TestRecord piggyBackTestRecord = new TestRecord();
      piggyBackTestRecord.testString = new Utf8("send-me-a-postcard-now");
      piggyBackTestRecord.testInt = 667;
      piggyBackProxy.test(piggyBackTestRecord);

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
      piggybackTransmitter.stop();
      piggybackReceiver.stop();
      transmitter.stop();
      receiver.stop();
    }
  }

  // Tests.

  @Test
  public void testLocalProtocolRegistryHTTP() throws Exception {

    // Test old HTTP.
    basic(new LocalProtocolRegistry(),
        new ReceiverPiggybackTransmitterFactory() {
          @Override
          public ReceiverPiggybackTransmitter create() {
            try {
              return new HTTPTransmitter(new URI("http://localhost:23663"), true);
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }
        },
        new TransmitterPiggybackReceiverFactory() {
          @Override
          public TransmitterPiggybackReceiver create(Consumer consumer) {
            return new HTTPReceiver(consumer, "localhost", 23663, true, false);
          }
        });
  }

//  @Test
//  public void testLocalProtocolRegistryNettyHTTP() throws Exception {
//
//    // Test Netty HTTP.
//    basic(new LocalProtocolRegistry(),
//        new ReceiverPiggybackTransmitterFactory() {
//          @Override
//          public ReceiverPiggybackTransmitter create() {
//            return new NettyTransmitter("localhost", 23660, true,
//                true, false, true, true,
//                null, null, null, null);
//          }
//        },
//        new TransmitterPiggybackReceiverFactory() {
//          @Override
//          public TransmitterPiggybackReceiver create(Consumer consumer) {
//            return new NettyReceiver(
//                consumer, "localhost", 23663,
//                true, false, true, true,
//                null, null, null, null);
//          }
//        });
//  }

  @Test
  public void testZooKeeperProtocolRegistryHTTP() {
    try {
      createZooKeeper();
      basic(new ZooKeeperProtocolRegistry("localhost:22181", "/avro/test/protocol"),
          new ReceiverPiggybackTransmitterFactory() {
            @Override
            public ReceiverPiggybackTransmitter create() {
              try {
                return new HTTPTransmitter(new URI("http://localhost:23663"), true);
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            }
          },
          new TransmitterPiggybackReceiverFactory() {
            @Override
            public TransmitterPiggybackReceiver create(Consumer consumer) {
              return new HTTPReceiver(consumer, "localhost", 23663, true, false);
            }
          });
    } catch (Exception ioe) {
      throw new RuntimeException(ioe);
    }
  }

//  @Test
//  public void testZooKeeperProtocolRegistryNettyHTTP() {
//    try {
//      createZooKeeper();
//      basic(new ZooKeeperProtocolRegistry("localhost:22181", "/avro/test/protocol"),
//          new ReceiverPiggybackTransmitterFactory() {
//            @Override
//            public ReceiverPiggybackTransmitter create() {
//              return new NettyTransmitter("localhost", 23660, true,
//                  true, false, true, true,
//                  null, null, null, null);
//            }
//          },
//          new TransmitterPiggybackReceiverFactory() {
//            @Override
//            public TransmitterPiggybackReceiver create(Consumer consumer) {
//              return new NettyReceiver(
//                  consumer, "localhost", 23663,
//                  true, false, true, true,
//                  null, null, null, null);
//            }
//          });
//    } catch (Exception ioe) {
//      throw new RuntimeException(ioe);
//    }
//  }

  // Inner classes.

  public interface ReceiverPiggybackTransmitterFactory {
    ReceiverPiggybackTransmitter create();
  }

  public interface TransmitterPiggybackReceiverFactory {
    TransmitterPiggybackReceiver create(Consumer consumer);
  }

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
