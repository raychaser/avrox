package org.digitalresearch.avrox.messaging;

import java.io.IOException;

import org.apache.avro.*;
import org.apache.avro.io.*;

/**
 * @author Christian (raychaser@gmail.com)
 */
public interface Producer {

  // Interface.

  void writeRequest(Schema schema, Object request, Encoder out) throws IOException;

  void addMessagingPlugin(MessagingPlugin plugin);

  void produce(String messageName, Object request) throws Exception;
}
