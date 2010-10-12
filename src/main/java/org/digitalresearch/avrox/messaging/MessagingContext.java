package org.digitalresearch.avrox.messaging;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.*;
import org.apache.avro.util.*;

/**
 * @author Christian (raychaser@gmail.com)
 */
public class MessagingContext {

  // Instance fields.

  protected Map<Utf8, ByteBuffer> messageMeta;

  private Protocol.Message message;
  List<ByteBuffer> messagePayload;

  // Implementation.

  public synchronized Map<Utf8, ByteBuffer> getMessageMeta() {
    if (messageMeta == null) messageMeta = new HashMap<Utf8, ByteBuffer>();
    return messageMeta;
  }

  public synchronized void setMessageMeta(Map<Utf8, ByteBuffer> newmeta) {
    messageMeta = newmeta;
  }

  public void setMessage(Protocol.Message message) {
    this.message = message;
  }

  public Protocol.Message getMessage() {
    return message;
  }

  public void setMessagePayload(List<ByteBuffer> payload) {
    this.messagePayload = payload;
  }

  public List<ByteBuffer> getMessagePayload() {
    return this.messagePayload;
  }
}