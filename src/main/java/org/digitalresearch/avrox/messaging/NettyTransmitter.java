package org.digitalresearch.avrox.messaging;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import org.hornetq.core.remoting.impl.ssl.*;
import org.jboss.netty.bootstrap.*;
import org.jboss.netty.buffer.*;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.group.*;
import org.jboss.netty.channel.socket.nio.*;
import org.jboss.netty.channel.socket.oio.*;
import org.jboss.netty.handler.codec.compression.*;
import org.jboss.netty.handler.codec.embedder.*;
import org.jboss.netty.handler.codec.http.*;
import org.jboss.netty.handler.ssl.*;
import org.jboss.netty.util.*;
import org.slf4j.*;

import static org.jboss.netty.buffer.ChannelBuffers.*;

/**
 * @author Christian (raychaser@gmail.com)
 */
public class NettyTransmitter implements ReceiverPiggybackTransmitter {

  // Log.

  protected static final Logger LOG = LoggerFactory.getLogger(NettyTransmitter.class);

  // Instance fields.

  protected final String host;
  protected final int port;
  protected final boolean durable;
  protected final boolean compress;
  protected final boolean ssl;
  protected final boolean nio;
  protected final boolean http;
  protected final String keystorePath;
  protected final String keystorePassword;
  protected final String truststorePath;
  protected final String truststorePassword;
  protected final String connectionString;

  protected final long httpMaxClientIdleTime = 500;
  private final AtomicBoolean connectLock = new AtomicBoolean(false);

  protected ChannelFactory channelFactory;
  protected ExecutorService executorService;
  protected volatile ChannelGroup channelGroup;
  protected ClientBootstrap bootstrap;
  protected Connection connection;

  protected Receiver responseMessageReceiver;

  // Implementation.

  public NettyTransmitter(String host, int port, boolean durable,
                          boolean compress, boolean ssl, boolean nio, boolean http,
                          String keystorePath, String keystorePassword,
                          String truststorePath, String truststorePassword) {
    this.host = host;
    this.port = port;
    this.durable = durable;
    this.compress = compress;
    this.ssl = ssl;
    this.nio = nio;
    this.http = http;
    this.keystorePath = keystorePath;
    this.keystorePassword = keystorePassword;
    this.truststorePath = truststorePath;
    this.truststorePassword = truststorePassword;
    this.connectionString = String.format("%s:%d", host, port);
  }

  protected ExecutorService createExecutorService() {
    // Consider overriding in subclass, this pool is unbounded!
    ExecutorService result = Executors.newCachedThreadPool();
    return result;
  }

  protected void connect() {

    // Bail out if we are already connected.
    if (connection != null) return;

    // Lock.
    while (!connectLock.compareAndSet(false, true)) {
      Thread.yield();
    }

    try {

      // Connect to the server.
      SocketAddress address = new InetSocketAddress(host, port);
      ChannelFuture future = bootstrap.connect(address);
      future.awaitUninterruptibly();

      // What happened?
      if (future.isSuccess()) {

        // Get the channel and do any SSL processing.
        Channel channel = future.getChannel();
        SslHandler sslHandler = channel.getPipeline().get(SslHandler.class);
        if (sslHandler != null) {
          ChannelFuture handshakeFuture = sslHandler.handshake();
          handshakeFuture.awaitUninterruptibly();
          if (handshakeFuture.isSuccess()) {
            channel.getPipeline().get(ClientChannelHandler.class).active = true;
          } else {
            channel.close().awaitUninterruptibly();
            return;
          }
        } else {
          channel.getPipeline().get(ClientChannelHandler.class).active = true;
        }

        // Remember the connection.
        connection = new Connection(channel, compress);

      } else {

        // This is not good.
        LOG.error("Failed to create netty connection", future.getCause());

        // Return nothing.
        return;
      }
    } finally {

      // Unlock.
      connectLock.set(false);
    }
  }

  protected void disconnect() {

  }

  // ReceiverPiggybackTransmitter implementation.

  @Override
  public void addResponseMessageReceiver(Receiver responseMessageReceiver) {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  // Transmitter implementation.

  @Override
  public void start() {

    // Configure the executor services.
    executorService = createExecutorService();
    VirtualExecutorService bossExecutor = new VirtualExecutorService(executorService);
    VirtualExecutorService workerExecutor = new VirtualExecutorService(executorService);

    // NIO requested, or old thread-based IO?
    if (nio) {
      channelFactory = new NioClientSocketChannelFactory(bossExecutor, workerExecutor);
    } else {
      channelFactory = new OioClientSocketChannelFactory(workerExecutor);
    }

    // Bind and start to accept incoming connections.
    bootstrap = new ClientBootstrap(channelFactory);

    // Setup SSL if requested.
    final SSLContext context;
    if (ssl) {
      try {
        context = SSLSupport.getInstance(
            true, keystorePath, keystorePassword, null, null);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    } else {
      context = null;
    }

    bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
      public ChannelPipeline getPipeline() throws Exception {

        // The handlers.
        List<ChannelHandler> handlers = new ArrayList<ChannelHandler>();

        // Do we need to worry about SSL?
        if (ssl) {
          SSLEngine engine = context.createSSLEngine();
          engine.setUseClientMode(true);
          engine.setWantClientAuth(true);
          SslHandler handler = new SslHandler(engine);
          handlers.add(handler);
        }

        // What about HTTP?
        if (http) {
          handlers.add(new HttpRequestEncoder());
          handlers.add(new HttpResponseDecoder());
          handlers.add(new HttpHandler());
        }

        // The message decoder.
        handlers.add(new NettyMessageDecoder());

        // The channel handler.
        handlers.add(new ClientChannelHandler(channelGroup));

        // Create and return the result.
        ChannelPipeline result = new StaticChannelPipeline(
            handlers.toArray(new ChannelHandler[handlers.size()]));
        return result;
      }
    });


    bootstrap.setOption("tcpNoDelay", true);
//    bootstrap.setOption("receiveBufferSize", tcpReceiveBufferSize);
//    bootstrap.setOption("sendBufferSize", tcpSendBufferSize);
    bootstrap.setOption("reuseAddress", true);
    bootstrap.setOption("keepAlive", true);

    // Setup the channel groups.
    channelGroup = new DefaultChannelGroup("NettyTransmitter");
  }

  @Override
  public void writeBuffers(List<ByteBuffer> buffers) throws IOException {

    // Write the data.
    connect();
    connection.write(buffers);
  }

  @Override
  public void stop() {
    disconnect();
  }

  // Inner classes.

  protected static class Connection {

    // Instance fields.

    protected final Channel channel;
    protected final boolean compress;

    private final AtomicBoolean writeLock = new AtomicBoolean(false);

    // Implementation.

    public Connection(Channel channel, boolean compress) {
      this.channel = channel;
      this.compress = compress;
    }

    public void write(List<ByteBuffer> buffers) {

      // Lock.
      while (!writeLock.compareAndSet(false, true)) {
        Thread.yield();
      }

      try {

        byte[] digest = null;
        try {
          MessageDigest m = MessageDigest.getInstance("MD5");
          m.reset();
          for (int i = 0; i < buffers.size(); i++) {
            ByteBuffer buffer = buffers.get(i);
            m.update(buffer.array(), buffer.arrayOffset(), buffer.remaining());
          }
          digest = m.digest();
        } catch (NoSuchAlgorithmException nsae) {
          LOG.info("Error getting MD5", nsae);
        }

        // Wrap the input buffers for Netty.
        ChannelBuffer[] channelBuffers = new ChannelBuffer[buffers.size()];
        for (int i = 0; i < buffers.size(); i++)
          channelBuffers[i] = wrappedBuffer(buffers.get(i));
        ChannelBuffer content = wrappedBuffer(channelBuffers);

        // Create the frame and version and content headers.
        ChannelBuffer frame = ChannelBuffers.buffer(27);
        frame.writeByte('F');
        frame.writeByte('V');
        frame.writeByte(1);
        frame.writeByte(1);

        // Compression?
        frame.writeByte('C');
        if (compress) {
          frame.writeByte(1);
          EncoderEmbedder encoder = new EncoderEmbedder(new ZlibEncoder(6));
          encoder.offer(content);
          content = (ChannelBuffer) encoder.poll();
          encoder.finish();
        } else {
          frame.writeByte(0);
        }

        // Checksum.
        frame.writeByte('D');
        frame.writeBytes(digest);

        // Write the byte count.
        int byteCount = content.readableBytes();
        frame.writeInt(byteCount);

        // Assemble the packet and write to the channel.
        ChannelBuffer[] frameAndContent = new ChannelBuffer[2];
        frameAndContent[0] = frame;
        frameAndContent[1] = content;
        ChannelBuffer packet = wrappedBuffer(frameAndContent);
        ChannelFuture future = channel.write(packet);

        boolean flush = true;
        if (flush) {
          while (true) {
            try {
              boolean ok = future.await(10000);
              if (!ok) {
                LOG.warn("Timed out waiting for packet to be flushed");
              }
              break;
            }
            catch (InterruptedException ignore) {
            }
          }
        }

      } finally {

        // Unlock.
        writeLock.set(false);
      }
    }
  }

  protected class ClientChannelHandler extends SimpleChannelHandler {

    // Log.

    protected final Logger LOG = LoggerFactory.getLogger(ClientChannelHandler.class);

    // Instance fields.

    protected final ChannelGroup group;

    protected volatile boolean active;

    // Implementation.

    public ClientChannelHandler(ChannelGroup group) {
      this.group = group;
    }

    // SimpleChannelHandler implementation.

    @Override
    public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
      LOG.debug("Channel open, ctx: '{}', event: {}", ctx.getName(), e);
      group.add(e.getChannel());
      ctx.sendUpstream(e);
    }

    @Override
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
      LOG.debug("Channel connected, ctx: '{}', event: {}", ctx.getName(), e);
      SslHandler sslHandler = ctx.getPipeline().get(SslHandler.class);
      if (sslHandler != null) {
        sslHandler.handshake().addListener(new ChannelFutureListener() {
          public void operationComplete(ChannelFuture future) throws Exception {
            if (future.isSuccess()) {
              active = true;
            } else {
              future.getChannel().close();
            }
          }
        });
      } else {
        active = true;
      }
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
      LOG.debug("Message received, ctx: '{}', event: {}", ctx.getName(), e);
//      ChannelBuffer buffer = (ChannelBuffer) e.getMessage();
    }

    @Override
    public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
      LOG.debug("Channel disconnected, ctx: '{}', event: {}", ctx.getName(), e);
      synchronized (this) {
        if (active) {
          active = false;
        }
      }
    }

    @Override
    public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
      LOG.debug("Channel closed, ctx: '{}', event: {}", ctx.getName(), e);
      active = false;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
      LOG.error("Exception caught", e.getCause());
      synchronized (this) {
        if (!active) {
          return;
        }
      }
    }
  }

  protected class HttpHandler extends SimpleChannelHandler {
    private Channel channel;
    private long lastSendTime = 0;
    private boolean waitingGet = false;
//    private HttpIdleTimer task;
    private final String url = "http://" + host + ":" + port + "/";
//    private final org.hornetq.utils.Future handShakeFuture = new org.hornetq.utils.Future();
//    private boolean active = false;
//    private boolean handshaking = false;
//    private final CookieDecoder cookieDecoder = new CookieDecoder();
//    private String cookie;
//    private final CookieEncoder cookieEncoder = new CookieEncoder(false);

    @Override
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e)
        throws Exception {
      super.channelConnected(ctx, e);
      channel = e.getChannel();
//         if (httpClientIdleScanPeriod > 0)
//         {
//            task = new HttpIdleTimer();
//            java.util.concurrent.Future<?> future = scheduledThreadPool.scheduleAtFixedRate(task,
//                                                                                            httpClientIdleScanPeriod,
//                                                                                            httpClientIdleScanPeriod,
//                                                                                            TimeUnit.MILLISECONDS);
//            task.setFuture(future);
//         }
    }

    @Override
    public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
//      if (task != null) {
//        task.close();
//      }
      super.channelClosed(ctx, e);
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
      HttpResponse response = (HttpResponse) e.getMessage();
//         if (httpRequiresSessionId && !active)
//         {
//            Set<Cookie> cookieMap = cookieDecoder.decode(response.getHeader(HttpHeaders.Names.SET_COOKIE));
//            for (Cookie cookie : cookieMap)
//            {
//               if (cookie.getName().equals("JSESSIONID"))
//               {
//                  cookieEncoder.addCookie(cookie);
//                  this.cookie = cookieEncoder.encode();
//               }
//            }
//            active = true;
//            handShakeFuture.run();
//         }
      MessageEvent event = new UpstreamMessageEvent(
          e.getChannel(), response.getContent(), e.getRemoteAddress());
      waitingGet = false;
      ctx.sendUpstream(event);
    }

    @Override
    public void writeRequested(final ChannelHandlerContext ctx, final MessageEvent e) throws Exception {
      if (e.getMessage() instanceof ChannelBuffer) {
//        if (httpRequiresSessionId && !active) {
//          if (handshaking) {
//            handshaking = true;
//          } else {
//            if (!handShakeFuture.await(5000)) {
//              throw new RuntimeException("Handshake failed after timeout");
//            }
//          }
//        }

        HttpRequest httpRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, url);
//        if (cookie != null) {
//          httpRequest.addHeader(HttpHeaders.Names.COOKIE, cookie);
//        }
        ChannelBuffer buf = (ChannelBuffer) e.getMessage();
        httpRequest.setContent(buf);
        httpRequest.addHeader(HttpHeaders.Names.CONTENT_LENGTH, String.valueOf(buf.writerIndex()));
        Channels.write(ctx, e.getFuture(), httpRequest, e.getRemoteAddress());
        lastSendTime = System.currentTimeMillis();
      } else {
        Channels.write(ctx, e.getFuture(), e.getMessage(), e.getRemoteAddress());
        lastSendTime = System.currentTimeMillis();
      }
    }

    private class HttpIdleTimer implements Runnable {
      private boolean closed = false;

      private java.util.concurrent.Future<?> future;

      public synchronized void run() {
        if (closed) {
          return;
        }

        if (!waitingGet && System.currentTimeMillis() > lastSendTime + httpMaxClientIdleTime) {
          HttpRequest httpRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, url);
          waitingGet = true;
          channel.write(httpRequest);
        }
      }

      public synchronized void setFuture(final java.util.concurrent.Future<?> future) {
        this.future = future;
      }

      public void close() {
        if (future != null) {
          future.cancel(false);
        }

        closed = true;
      }
    }
  }
}
