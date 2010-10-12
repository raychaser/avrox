package org.digitalresearch.avrox.messaging;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.*;
import org.apache.avro.ipc.*;

/**
 * A local protocol registry. Used as a delegate by the ZooKeeper protocol
 * registry. Since endpoints are commonly in different events, this class
 * is otherwise mostly useful for tests.
 *
 * @author Christian (raychaser@gmail.com)
 */
public class LocalProtocolRegistry implements ProtocolRegistry {

  // Instance fields.

  protected final Map<MD5, Protocol> protocolByMD5 =
      Collections.synchronizedMap(new HashMap<MD5, Protocol>());

  // ProtocolRegistry implementation.

  @Override
  public boolean contains(MD5 md5) {
    return protocolByMD5.containsKey(md5);
  }

  @Override
  public Protocol get(MD5 md5) {
    return protocolByMD5.get(md5);
  }

  @Override
  public void register(Protocol protocol) {
    MD5 md5 = new MD5();
    md5.bytes(protocol.getMD5());
    protocolByMD5.put(md5, protocol);
  }
}
