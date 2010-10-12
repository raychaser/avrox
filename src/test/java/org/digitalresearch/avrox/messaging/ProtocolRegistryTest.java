package org.digitalresearch.avrox.messaging;

import java.util.List;

import org.apache.avro.Protocol;
import org.apache.avro.ipc.MD5;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;

import org.digitalresearch.avrox.TestProtocol;

import static junit.framework.Assert.assertEquals;

/**
 * @author Christian (raychaser@gmail.com)
 */
public class ProtocolRegistryTest extends ZooKeeperSupport {

  // Implementation.

  protected void basic(ProtocolRegistry protocolRegistry) {
    {
      // Add and get.
      Protocol thisProtocol = TestProtocol.PROTOCOL;
      protocolRegistry.register(thisProtocol);
      MD5 thisMD5 = new MD5();
      thisMD5.bytes(thisProtocol.getMD5());
      Protocol thatProtocol = protocolRegistry.get(thisMD5);
      assertEquals(thisProtocol, thatProtocol);
    }

    {
      // Add again!
      Protocol thisProtocol = TestProtocol.PROTOCOL;
      protocolRegistry.register(thisProtocol);
      MD5 thisMD5 = new MD5();
      thisMD5.bytes(thisProtocol.getMD5());
      Protocol thatProtocol = protocolRegistry.get(thisMD5);
      assertEquals(thisProtocol, thatProtocol);
    }
  }

  protected void deleteIfExists(ZooKeeper zk, String path) throws Exception {
    Stat stat = zk.exists(path, this);
    if (stat != null) {
      List<String> children = zk.getChildren(path, this);
      for (String child : children) {
        String childPath = String.format("%s/%s", path, child);
        Stat childStat = zk.exists(childPath, null);
        zk.delete(childPath, childStat.getVersion());
      }
      zk.delete(path, stat.getVersion());
    }
  }

  // Tests.

  @Test
  public void testLocalProtocolRegistry() {
    basic(new LocalProtocolRegistry());
  }

  @Test
  public void testZooKeeperProtocolRegistry() {
    try {
      ZooKeeper zk = createZooKeeper();

      // Get some coverage on the path creation code.
      deleteIfExists(zk, "/avro/test/protocol");
      deleteIfExists(zk, "/avro/test");
      deleteIfExists(zk, "/avro");

      // Off to testing...
      basic(new ZooKeeperProtocolRegistry("localhost:22181", "/avro/test/protocol"));
      basic(new ZooKeeperProtocolRegistry("localhost:22181", "/avro/test/protocol"));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
