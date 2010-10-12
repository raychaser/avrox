package org.digitalresearch.avrox.messaging;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.net.ssl.SSLEngine;

import org.jboss.netty.bootstrap.*;
import org.jboss.netty.buffer.*;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.nio.*;
import org.jboss.netty.handler.codec.compression.*;
import org.jboss.netty.handler.codec.embedder.*;
import org.jboss.netty.handler.codec.http.*;
import org.jboss.netty.handler.ssl.*;
import org.slf4j.*;

import static org.jboss.netty.buffer.ChannelBuffers.*;
import static org.jboss.netty.channel.Channels.*;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.*;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Values.*;
import static org.jboss.netty.handler.codec.http.HttpMethod.*;
import static org.jboss.netty.handler.codec.http.HttpVersion.*;

/**
 * A Transmitter that binds to a transport based on the HTTP protocol.
 *
 * @author Christian (raychaser@gmail.com)
 */
@SuppressWarnings("PMD.TooManyStaticImports")
public class HTTPTransmitter implements ReceiverPiggybackTransmitter {

  // Log.

  protected static final Logger LOG = LoggerFactory.getLogger(HTTPTransmitter.class);

  // Instance fields.

  protected final URI uri;
  protected final String scheme;
  protected final String host;
  protected int port;
  protected final boolean compress;
  protected final boolean ssl;

  protected AtomicBoolean connected = new AtomicBoolean(false);
  protected ClientBootstrap bootstrap;
  protected Channel channel;

  protected int bossQueueSize = 2;
  protected int workQueueSize = 2;
  protected LinkedBlockingQueue bossQueue = new LinkedBlockingQueue(bossQueueSize);
  protected LinkedBlockingQueue workQueue = new LinkedBlockingQueue(workQueueSize);

  protected int writeCounter;
  protected int writesPerKeepAlive = 1000;

  protected Receiver responseMessageReceiver;

  // Implementation.

  public HTTPTransmitter(URI uri, boolean compress) {
    this.uri = uri;
    this.scheme = uri.getScheme();
    this.host = uri.getHost();
    this.port = uri.getPort();
    this.compress = compress;

    if (port == -1)
      if (scheme.equalsIgnoreCase("http")) port = 80;
      else if (scheme.equalsIgnoreCase("https")) port = 443;

    // Make sure the scheme is valid.
    if (!scheme.equalsIgnoreCase("http") && !scheme.equalsIgnoreCase("https"))
      throw new RuntimeException("Only HTTP(S) is supported!");

    // SSL [ ] yes / [ ] no?
    this.ssl = scheme.equalsIgnoreCase("https");

    // Configure the client.
    bootstrap = new ClientBootstrap(
        new NioClientSocketChannelFactory(createBossPool(), createWorkPool()));

    // Set up the event pipeline factory.
    bootstrap.setPipelineFactory(new HttpClientPipelineFactory(ssl));
  }

  protected synchronized void connect() {

    // Ignore if we're already connected.
    if (connected.get()) return;

    long sleepMillis = 500;
    int maxTries = 10;
    int count = 0;
    while (!connected.get() && count < maxTries) {
      try {

        // Start the connection attempt.
        ChannelFuture future = bootstrap.connect(new InetSocketAddress(host, port));

        // Wait until the connection attempt succeeds or fails.
        channel = future.awaitUninterruptibly().getChannel();
        if (future.isSuccess()) {
          connected.set(true);
        } else {
          connected.set(false);
          bootstrap.releaseExternalResources();
          LOG.error("Error connecting to '" + uri + "' after '" + (count + 1) + "' attempts.",
              future.getCause());
          sleep(sleepMillis);
        }
      } catch (Throwable t) {
        sleep(sleepMillis);
      }

      // This counts as an attempt...
      count += 1;
    }

    // Connect unsuccessful?
    if (!connected.get()) throw new RuntimeException(String.format(
        "Error connecting to '%s' after '%d' tries", uri, maxTries));
  }

  protected ExecutorService createBossPool() {
    ExecutorService result = new ThreadPoolExecutor(1, 3,
        0L, TimeUnit.MILLISECONDS,
        bossQueue,
        new ThreadFactory() {
          int count;

          @Override
          public Thread newThread(Runnable r) {
            String name = String.format("Netty-Boss-%d", count++);
            LOG.info("Creating new Netty boss thread: " + name);
            return new Thread(r, name);
          }
        });
    return result;
  }

  protected ExecutorService createWorkPool() {
    ExecutorService result = new ThreadPoolExecutor(1, 3,
        0L, TimeUnit.MILLISECONDS,
        workQueue,
        new ThreadFactory() {
          int count;

          @Override
          public Thread newThread(Runnable r) {
            String name = String.format("Netty-Work-%d", count++);
            LOG.info("Creating new Netty work thread: " + name);
            return new Thread(r, String.format("Netty-Work-%d", count++));
          }
        });
    return result;
  }

  protected SSLEngine createSSLEngine() {
    SSLEngine result = DefaultSSLContextFactory.getClientContext().createSSLEngine();
    result.setUseClientMode(true);
    return result;
  }

  protected void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException ie) {
      LOG.warn("Interrupted during sleep", ie);
    }
  }

  public int getAggregatorBytes() {
    return 1 * 1024 * 1024;
  }

  // ReceiverPipelineTransmitter implementation.

  public void addResponseMessageReceiver(Receiver responseMessageReceiver) {
    this.responseMessageReceiver = responseMessageReceiver;
  }

  // Transmitter implementation.

  @Override
  public void start() {
    connect();
  }

  @Override
  public synchronized void writeBuffers(List<ByteBuffer> buffers) throws IOException {

    // Connect if necessary.
    if (!connected.get()) connect();

    // Is this the last cycle of this round of keep-alive?
    if (++writeCounter > writesPerKeepAlive) {
      writeCounter = -1;
    }

    // Prepare the HTTP request.
    HttpRequest request = new DefaultHttpRequest(HTTP_1_1, POST, uri.toASCIIString());
    request.setHeader(HOST, host);
    request.setHeader(ACCEPT_ENCODING, DEFLATE);

    // Keep the connection alive.
    HttpHeaders.setKeepAlive(request, writeCounter != -1);

    // Add the content and let's not forget to set the length.
    ChannelBuffer[] channelBuffers = new ChannelBuffer[buffers.size()];
    for (int i = 0; i < channelBuffers.length; i++)
      channelBuffers[i] = wrappedBuffer(buffers.get(i));
    ChannelBuffer content = wrappedBuffer(channelBuffers);

    // Compression requested?
    if (compress) {

      // Encode the request content using ZLib deflate. Technically, it
      // should be possible to accomplish this using the pipeline, but
      // i was so far unable to do that.
      EncoderEmbedder encoder = new EncoderEmbedder(new ZlibEncoder());
      encoder.offer(content);
      content = (ChannelBuffer) encoder.poll();
      encoder.finish();
      request.setHeader(CONTENT_ENCODING, DEFLATE);
    }

    request.setContent(content);
    HttpHeaders.setContentLength(request, content.readableBytes());

    try {

      // Send the HTTP request.
      channel.write(request);

      // Disconnect?
      if (writeCounter == -1) connected.set(false);

    } catch (Throwable t) {
      LOG.warn("Exception writing to channel. Closing connection.", t);
      connected.set(false);
    }
  }


  @Override
  public synchronized void stop() {

    // If we're connected and actually have a channel, close it.
    connected.set(false);
    if (channel != null) {
      channel.close();
      channel = null;
    }

    // And clean up all the remaining stuff.
    bootstrap.releaseExternalResources();
  }

  // Inner classes.

  public class HttpClientPipelineFactory implements ChannelPipelineFactory {

    // Instance fields.

    private final boolean ssl;

    // Implementation.

    public HttpClientPipelineFactory(boolean ssl) {
      this.ssl = ssl;
    }

    // ChannelPipelineFactory implementation.

    public ChannelPipeline getPipeline() throws Exception {

      // Create a default pipeline implementation.
      ChannelPipeline pipeline = pipeline();

      // Enable HTTPS if necessary.
      if (ssl) {
        SSLEngine engine = createSSLEngine();
        pipeline.addLast("ssl", new SslHandler(engine));
      }
      pipeline.addLast("codec", new HttpClientCodec());
      pipeline.addLast("inflater", new HttpContentDecompressor());
      pipeline.addLast("aggregator", new HttpChunkAggregator(getAggregatorBytes()));
      pipeline.addLast("handler", new HttpResponseHandler());
      return pipeline;
    }
  }

  public class HttpResponseHandler extends SimpleChannelUpstreamHandler {

    // SimpleChannelUpstreamHandler implementation.

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
      HttpResponse response = (HttpResponse) e.getMessage();
      if (response.getStatus().getCode() != 200) {
        LOG.error("Response from {} is not 200: {}", response, response.getStatus());
        // TODO: Handle this.
        return;
      }

      // Read the response.
      ChannelBuffer content = response.getContent();
      int messageCount = content.readInt();
      for (int i = 0; i < messageCount; i++) {

        // Read the message length.
        int messageLength = content.readInt();
        byte[] messageBytes = new byte[messageLength];
        content.readBytes(messageBytes, 0, messageLength); // TODO: Can avoid this copy?

        // Handle the message.
        if (responseMessageReceiver != null) {
          List<ByteBuffer> buffers = new ArrayList<ByteBuffer>(1);
          buffers.add(ByteBuffer.wrap(messageBytes));
          responseMessageReceiver.dispatch(buffers);
        }
      }
    }
  }
}
