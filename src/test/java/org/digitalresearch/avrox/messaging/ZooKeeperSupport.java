package org.digitalresearch.avrox.messaging;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.quorum.QuorumPeerMain;

/**
 * @author Christian (raychaser@gmail.com)
 */
public class ZooKeeperSupport implements Watcher {


  // Implementation.

  public ZooKeeper createZooKeeper() {

    ZooKeeper result = null;
    try {

      // Is ZooKeeper there?
      result = new ZooKeeper("localhost:22181", 3000, this);
      result.exists("/", null);

    } catch (Exception e) {

      try {

        // ZooKeeper is not there. Make it be there.
        new Thread(new Runnable() {
          public void run() {

            // Start up a ZooKeeper on a funny port.
            String dataDir = "zk-tmp";
            QuorumPeerMain.main(new String[]{"22181", dataDir});
          }
        }).start();

        // Wait for ZooKeeper to have started.
        Thread.sleep(1000); // TODO: Hairy, of course, but Works On My MachineTM

      } catch (Exception e2) {
        throw new RuntimeException(e2);
      }
    }

    return result;
  }

  // Watcher implementation.

  @Override
  public void process
      (WatchedEvent
          event) {
    // NOOP
  }
}
