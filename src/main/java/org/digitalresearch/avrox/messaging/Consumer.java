package org.digitalresearch.avrox.messaging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.avro.*;
import org.apache.avro.io.*;

/**
 * @author Christian (raychaser@gmail.com)
 */
public interface Consumer {

  // Interface.

  /**
   * Adds a new plugin to manipulate messaging meta data. Plugins are executed
   * in the order that they are added.
   *
   * @param plugin A plugin that will manipulate messaging meta data.
   */
  void addMessagingPlugin(MessagingPlugin plugin);

  void consume(List<ByteBuffer> buffers) throws IOException;

  Object readRequest(Schema schema, Decoder in) throws IOException;

  void consume(Protocol.Message message, Object request) throws Exception;
}
