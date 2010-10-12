package org.digitalresearch.avrox.messaging;

import org.apache.avro.*;
import org.apache.avro.ipc.*;

/**
 * A protocol registry can be shared by Producers and Consumers to stay on
 * top of which protocol are used for communication. At it's core, a
 * ProtocolRegistry is nothing more than a Map from MD5(Protocol) to Protocol.
 *
 * @author Christian (raychaser@gmail.com)
 */
public interface ProtocolRegistry {

  // Interface.

  /**
   * Whether this protocol registry knows of a protocol with the specified MD5.
   *
   * @param md5 The MD5 of the protocol.
   * @return The protocol matching the specified MD5.
   */
  boolean contains(MD5 md5);

  /**
   * The protocol with the specified MD5, or null if there's no such protocol.
   *
   * @param md5 The MD5 of the protocol to return.
   * @return The protocol with the specified MD5, or null if there's no such protocol.
   */
  Protocol get(MD5 md5);

  /**
   * Register a protocol.
   *
   * @param protocol The protocol to register.
   */
  void register(Protocol protocol);
}
