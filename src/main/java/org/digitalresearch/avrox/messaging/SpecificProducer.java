package org.digitalresearch.avrox.messaging;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import org.apache.avro.*;
import org.apache.avro.io.*;
import org.apache.avro.specific.*;

/**
 * A producer that uses Java classes generated from Avro schemas.
 *
 * @author Christian (raychaser@gmail.com)
 */
public class SpecificProducer extends AbstractProducer implements InvocationHandler {

  // Static implementation.

  public static Object getClient(Class<?> iface, ProtocolRegistry protocolRegistry,
                                 Transmitter transmitter)
      throws IOException {
    return getClient(iface, protocolRegistry, transmitter, SpecificData.get());
  }

  public static Object getClient(Class<?> iface, ProtocolRegistry protocolRegistry,
                                 Transmitter transmitter, SpecificData specificData) {
    Protocol protocol = specificData.getProtocol(iface);
    return Proxy.newProxyInstance(iface.getClassLoader(),
        new Class[]{iface},
        new SpecificProducer(protocol, protocolRegistry, transmitter));
  }

  public static Object getClient(Class<?> iface, SpecificProducer producer) {
    return Proxy.newProxyInstance(iface.getClassLoader(), new Class[]{iface}, producer);
  }

  // Implementation.

  public SpecificProducer(
      Class<?> iface, ProtocolRegistry protocolRegistry, Transmitter transmitter) {
    this(SpecificData.get().getProtocol(iface), protocolRegistry, transmitter);
  }

  public SpecificProducer(
      Protocol protocol, ProtocolRegistry protocolRegistry, Transmitter transmitter) {
    super(protocol, protocolRegistry, transmitter);
  }

  protected DatumWriter<Object> getDatumWriter(Schema schema) {
    return new SpecificDatumWriter<Object>(schema);
  }

  // Producer implementation.

  @Override
  public void writeRequest(Schema schema, Object request, Encoder out) throws IOException {
    Object[] args = (Object[]) request;
    int i = 0;
    for (Schema.Field param : schema.getFields())
      getDatumWriter(param.schema()).write(args[i++], out);
  }

  // InvocationHandler implementation.

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    produce(method.getName(), args);
    return null;
  }
}
