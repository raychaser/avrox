package org.digitalresearch.avrox.messaging;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.avro.util.*;
import org.apache.zookeeper.*;
import org.hornetq.api.core.*;
import org.hornetq.api.core.client.*;
import org.hornetq.core.config.*;
import org.hornetq.core.config.impl.*;
import org.hornetq.core.remoting.impl.netty.*;
import org.hornetq.core.server.*;
import org.junit.*;

import org.digitalresearch.avrox.*;

import static org.junit.Assert.*;

/**
 * @author Christian (raychaser@gmail.com)
 */
public class HornetQMessagingTest extends ZooKeeperSupport {

  // Implementation.

  protected void basic(ProtocolRegistry protocolRegistry) throws Exception {

    // Connection parameters.
    String host = "localhost";
    int port = 65432;
    String address = "hqmsgtest";
    String queueName = "hqmsgtest-consumer";

    // Start embedded message broker.
    Configuration configuration = new ConfigurationImpl();
    configuration.setPersistenceEnabled(false);
    configuration.setSecurityEnabled(false);
    Map<String, Object> transportConfiguration = new HashMap<String, Object>();
    transportConfiguration.put("host", host);
    transportConfiguration.put("port", port);
    TransportConfiguration transpConf =
        new TransportConfiguration(
            NettyAcceptorFactory.class.getName(), transportConfiguration);
    HashSet<TransportConfiguration> transportConfigurations =
        new HashSet<TransportConfiguration>();
    transportConfigurations.add(transpConf);
    configuration.setAcceptorConfigurations(transportConfigurations);
    HornetQServer server = HornetQServers.newHornetQServer(configuration);
    server.start();

    // Create the queue we are going to use.
    ClientSessionFactory sf =
        HornetQClient.createClientSessionFactory(
            new TransportConfiguration(
                NettyConnectorFactory.class.getName(), transportConfiguration));
    ClientSession coreSession = sf.createSession(false, false, false);
    coreSession.createQueue(address, queueName);
    coreSession.close();

    // Create an HornetQ transmitter.
    HornetQTransmitter transmitter = new HornetQTransmitter(host, port, address, false, -1);
    transmitter.start();

    // Create a local receiver.
    TestProtocolHandler testProtocolHandler = new TestProtocolHandler();
    Consumer consumer = new SpecificConsumer(
        TestProtocol.class, protocolRegistry, testProtocolHandler);
    HornetQReceiver receiver = new HornetQReceiver(consumer, host, port, queueName);
    receiver.start();

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
      server.stop();
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
