package org.digitalresearch.avrox.messaging;

import java.io.FileInputStream;
import java.security.KeyStore;
import java.security.Security;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;

/**
 * @author Christian (raychaser@gmail.com)
 */
public class DefaultSSLContextFactory {

  private static final String PROTOCOL = "TLS";
  private static final SSLContext SERVER_CONTEXT;
  private static final SSLContext CLIENT_CONTEXT;

  static {
    String algorithm = Security.getProperty("ssl.KeyManagerFactory.algorithm");
    if (algorithm == null) algorithm = "SunX509";

    SSLContext serverContext = null;
    SSLContext clientContext = null;
    try {

      // Load the keystore.
      KeyStore ks = KeyStore.getInstance("JKS");
      FileInputStream fis = new FileInputStream("src/test/resources/cert.jks");
      try {
        ks.load(fis, "secret".toCharArray());
      } finally {
        fis.close();
      }

      // Set up key manager factory to use our key store
      KeyManagerFactory kmf = KeyManagerFactory.getInstance(algorithm);
      kmf.init(ks, "secret".toCharArray());

      // Initialize the SSLContext to work with our key managers.
      serverContext = SSLContext.getInstance(PROTOCOL);
      serverContext.init(kmf.getKeyManagers(), null, null);

    } catch (Exception e) {
      throw new Error("Failed to initialize the server-side SSLContext", e);
    }

    try {
      clientContext = SSLContext.getInstance(PROTOCOL);
      clientContext.init(null, DefaultTrustManagerFactory.getTrustManagers(), null);
    } catch (Exception e) {
      throw new Error("Failed to initialize the client-side SSLContext", e);
    }

    SERVER_CONTEXT = serverContext;
    CLIENT_CONTEXT = clientContext;
  }

  public static SSLContext getServerContext() {
    return SERVER_CONTEXT;
  }

  public static SSLContext getClientContext() {
    return CLIENT_CONTEXT;
  }
}
