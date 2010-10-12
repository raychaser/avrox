package org.digitalresearch.avrox.messaging;

import java.io.IOException;
import java.util.List;

import org.apache.avro.*;
import org.apache.avro.ipc.*;
import org.apache.commons.codec.binary.*;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.*;
import org.slf4j.*;

/**
 * A protocol registry using ZooKeeper. This is a very primitive implementation
 * and will very likely have to be redone for performance.
 *
 * @author Christian (raychaser@gmail.com)
 */
public class ZooKeeperProtocolRegistry implements ProtocolRegistry, Watcher {

  // Log.

  private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperProtocolRegistry.class);

  // Instance fields.

  protected final String zooKeeperConnectString;
  protected final String path;

  protected ZooKeeper zk;
  protected LocalProtocolRegistry delegate;

  // Implementation.

  public ZooKeeperProtocolRegistry(String zooKeeperConnectString, String path) {
    this.zooKeeperConnectString = zooKeeperConnectString;
    this.path = path;
    populate();
  }

  protected synchronized void populate() {
    int retries = 3;
    boolean success = false;
    Exception lastException = null;
    while (retries-- > 0) {
      ensureZooKeeper();
      try {

        // Get or create the path.
        Stat stat = zk.exists(path, this);
        if (stat == null) createPath();

        // Get all the children - those are the MD5s.
        LocalProtocolRegistry registry = new LocalProtocolRegistry();
        List<String> nodes = zk.getChildren(path, this);
        for (String node : nodes) {
          LOG.debug("ZK node: '{}'", new Object[]{node});
          byte[] data = zk.getData(String.format("%s/%s", path, node), this, null);
          String schema = new String(data, "UTF8");
          LOG.debug("    Schema: '{}'", new Object[]{schema});
          Protocol protocol = Protocol.parse(schema);
          registry.register(protocol);
        }

        // Update the delegate.
        delegate = registry;

        // Don't try again.
        retries = 0;
        success = true;

      } catch (Exception e) {
        lastException = e;
        LOG.error("Error talking to ZooKeeper", e);
      }
    }

    // Bail if we have to
    if (!success) {
      throw new RuntimeException("After retry", lastException);
    }
  }

  protected void ensureZooKeeper() {
    if (zk == null) {
      try {
        LOG.info("Creating ZooKeeper client for '{}'", zooKeeperConnectString);
        zk = new ZooKeeper(zooKeeperConnectString, 5000, this);
      } catch (IOException ioe) {
        LOG.error("During ZooKeeper client creation", ioe);
        throw new RuntimeException(ioe);
      }
    }

    try {
      zk.exists("/", this);
    } catch (Exception e) {
      try {
        LOG.info("Error getting '/' in ZooKeeper for '{}', error: '{}'",
            zooKeeperConnectString, e.getMessage());
        zk = new ZooKeeper(zooKeeperConnectString, 5000, this);
      } catch (IOException ioe) {
        zk = null;
        LOG.error("During ZooKeeper client creation", ioe);
        throw new RuntimeException(ioe);
      }
    }
  }

  protected void createPath() throws Exception {
    String[] pathComponents = path.split("/");
    StringBuilder pathBuilder = new StringBuilder();
    for (int i = 1; i < pathComponents.length; i++) {
      pathBuilder.append("/");
      pathBuilder.append(pathComponents[i]);
      String newPath = pathBuilder.toString();
      Stat stat = zk.exists(newPath, null);
      if (stat == null)
        zk.create(newPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }
  }

  // ProtocolRegistry implementation.

  @Override
  public boolean contains(MD5 md5) {
    populate();
    return delegate.contains(md5);
  }

  @Override
  public Protocol get(MD5 md5) {
    populate();
    return delegate.get(md5);
  }

  @Override
  public synchronized void register(Protocol protocol) {
    ensureZooKeeper();
    String md5Hex = new String(Hex.encodeHex(protocol.getMD5()));
    try {
      String localProtocolString = protocol.toString();
      String protocolPath = String.format("%s/%s", path, md5Hex);
      Stat stat = zk.exists(protocolPath, this);
      if (stat == null) {
        byte[] data = localProtocolString.getBytes("UTF8");
        zk.create(protocolPath, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      } else {
        byte[] data = zk.getData(protocolPath, this, null);
        String remoteProtocolString = new String(data, "UTF8");
        if (!remoteProtocolString.equals(localProtocolString))
          // TODO: Ooh La La!
          throw new RuntimeException(String.format(
              "ZooKeeper has protocol with MD5: '%s' as '%s', but local protocol " +
                  "with same MD5 is '%s'!", md5Hex, remoteProtocolString, localProtocolString));
      }
    } catch (Exception uee) {
      throw new RuntimeException(uee);
    }
    populate();
  }

  // Watcher implementation.

  @Override
  public void process(WatchedEvent event) {
  }
}
