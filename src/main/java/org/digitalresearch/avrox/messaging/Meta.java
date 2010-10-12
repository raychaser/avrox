package org.digitalresearch.avrox.messaging;

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.avro.*;
import org.apache.avro.generic.*;
import org.apache.avro.util.*;

/**
 * @author Christian (raychaser@gmail.com)
 */
public class Meta {

  // Static fields.

  public static final Schema META_SCHEMA =
      Schema.createMap(Schema.create(Schema.Type.BYTES));
  public static final ThreadLocal<GenericDatumReader<Map<Utf8, ByteBuffer>>> META_READER =
      new ThreadLocal<GenericDatumReader<Map<Utf8, ByteBuffer>>>() {
        @Override
        protected GenericDatumReader<Map<Utf8, ByteBuffer>> initialValue() {
          return new GenericDatumReader<Map<Utf8, ByteBuffer>>(META_SCHEMA);
        }
      };
  public static final ThreadLocal<GenericDatumWriter<Map<Utf8, ByteBuffer>>> META_WRITER =
      new ThreadLocal<GenericDatumWriter<Map<Utf8, ByteBuffer>>>() {
        @Override
        protected GenericDatumWriter<Map<Utf8, ByteBuffer>> initialValue() {
          return new GenericDatumWriter<Map<Utf8, ByteBuffer>>(META_SCHEMA);
        }
      };
}
