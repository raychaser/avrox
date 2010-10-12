package org.digitalresearch.avrox.messaging;

import java.io.IOException;
import java.lang.reflect.Method;

import org.apache.avro.*;
import org.apache.avro.io.*;
import org.apache.avro.specific.*;

/**
 * A consumer that uses Java classes generated from Avro schemas.
 *
 * @author Christian (raychaser@gmail.com)
 */
public class SpecificConsumer extends AbstractConsumer {

  // Instance fields.

  protected final Object impl;
  protected final SpecificData data;

  // Implementation,

  public SpecificConsumer(Class iface, ProtocolRegistry protocolRegistry, Object impl) {
    this(SpecificData.get().getProtocol(iface), protocolRegistry, impl);
  }

  public SpecificConsumer(Protocol protocol, ProtocolRegistry protocolRegistry, Object impl) {
    this(protocol, protocolRegistry, impl, SpecificData.get());
  }

  protected SpecificConsumer(Protocol protocol, ProtocolRegistry protocolRegistry,
                             Object impl, SpecificData data) {
    super(protocol, protocolRegistry);
    this.impl = impl;
    this.data = data;
  }

  protected DatumReader<Object> getDatumReader(Schema schema) {
    return new SpecificDatumReader<Object>(schema);
  }

  // Consumer implementation.

  @Override
  public Object readRequest(Schema schema, Decoder in) throws IOException {
    Object[] args = new Object[schema.getFields().size()];
    int i = 0;
    for (Schema.Field param : schema.getFields())
      args[i++] = getDatumReader(param.schema()).read(null, in);
    return args;
  }

  @Override
  public void consume(Protocol.Message message, Object request) throws Exception {
    Class[] paramTypes = new Class[message.getRequest().getFields().size()];
    int i = 0;
    try {
      for (Schema.Field param : message.getRequest().getFields())
        paramTypes[i++] = data.getClass(param.schema());
      Method method = impl.getClass().getMethod(message.getName(), paramTypes);
      method.setAccessible(true);
      method.invoke(impl, (Object[]) request);
    } catch (Throwable t) {
      throw new Exception(t);
    }
  }
}
