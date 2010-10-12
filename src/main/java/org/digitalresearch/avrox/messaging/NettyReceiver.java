package org.digitalresearch.avrox.messaging;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import org.hornetq.core.remoting.impl.ssl.*;
import org.jboss.netty.bootstrap.*;
import org.jboss.netty.buffer.*;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.group.*;
import org.jboss.netty.channel.socket.nio.*;
import org.jboss.netty.channel.socket.oio.*;
import org.jboss.netty.handler.codec.http.*;
import org.jboss.netty.handler.ssl.*;
import org.jboss.netty.util.*;
import org.slf4j.*;

/**
 * @author Christian (raychaser@gmail.com)
 */
public class NettyReceiver implements TransmitterPiggybackReceiver {

  // Log.

  protected static final Logger LOG = LoggerFactory.getLogger(NettyReceiver.class);

  // Instance fields.

  protected final Consumer consumer;
  protected final String host;
  protected final int port;
  protected final boolean compress;
  protected final boolean ssl;
  protected final boolean nio;
  protected final boolean http;
  protected final String keystorePath;
  protected final String keystorePassword;
  protected final String truststorePath;
  protected final String truststorePassword;
  protected final LinkedBlockingQueue<List<ByteBuffer>> responseMessageQueue =
      new LinkedBlockingQueue<List<ByteBuffer>>(100);

  protected ChannelFactory channelFactory;
  protected ExecutorService executorService;
  protected volatile ChannelGroup channelGroup;
  protected volatile ChannelGroup serverChannelGroup;
  protected ServerBootstrap bootstrap;
  protected HttpKeepAliveRunnable httpKeepAliveRunnable;

  // Implementation.

  public NettyReceiver(
      Consumer consumer, String host, int port,
      boolean compress, boolean ssl, boolean nio, boolean http,
      String keystorePath, String keystorePassword,
      String truststorePath, String truststorePassword) {
    this.consumer = consumer;
    this.host = host;
    this.port = port;
    this.compress = compress;
    this.ssl = ssl;
    this.nio = nio;
    this.http = http;
    this.keystorePath = keystorePath;
    this.keystorePassword = keystorePassword;
    this.truststorePath = truststorePath;
    this.truststorePassword = truststorePassword;
  }

  protected SSLEngine createSSLEngine() {
    SSLEngine result = DefaultSSLContextFactory.getServerContext().createSSLEngine();
    result.setUseClientMode(false);
    return result;
  }

  protected ExecutorService createExecutorService() {
    // Consider overriding in subclass, this pool is unbounded!
    ExecutorService result = Executors.newCachedThreadPool();
    return result;
  }

  // TransmitterPiggybackReceiver implementation.

  @Override
  public void queueResponseMessage(List<ByteBuffer> buffers) {
    try {
      responseMessageQueue.put(buffers);
    } catch (InterruptedException ie) {
      throw new RuntimeException(ie);
    }
  }

  // Receiver implementation.

  @Override
  public void start() {

    // Configure the executor services.
    executorService = createExecutorService();
    VirtualExecutorService bossExecutor = new VirtualExecutorService(executorService);
    VirtualExecutorService workerExecutor = new VirtualExecutorService(executorService);

    // NIO requested, or old thread-based IO?
    if (nio) {
      channelFactory = new NioServerSocketChannelFactory(bossExecutor, workerExecutor);
    } else {
      channelFactory = new OioServerSocketChannelFactory(bossExecutor, workerExecutor);
    }

    // Bind and start to accept incoming connections.
    bootstrap = new ServerBootstrap(channelFactory);

    // Setup SSL if requested.
    final SSLContext context;
    if (ssl) {
      try {
        context = SSLSupport.createServerContext(
            keystorePath, keystorePassword, truststorePath, truststorePassword);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    } else {
      context = null;
    }

    // Some additional setup for HTTP support.
    if (http) {
      ScheduledExecutorService scheduledExecutorService =
          new ScheduledThreadPoolExecutor(4);
      httpKeepAliveRunnable = new HttpKeepAliveRunnable();
      Future<?> future = scheduledExecutorService.scheduleAtFixedRate(
          httpKeepAliveRunnable, 5000, 5000, TimeUnit.MILLISECONDS);
      httpKeepAliveRunnable.setFuture(future);
    }

    // Create the pipeline factory.
    ChannelPipelineFactory factory = new ChannelPipelineFactory() {
      public ChannelPipeline getPipeline() throws Exception {

        // The handlers.
        Map<String, ChannelHandler> handlers = new LinkedHashMap<String, ChannelHandler>();

        // Do we need to deal with SSL?
        if (ssl) {
          SSLEngine engine = context.createSSLEngine();
          engine.setUseClientMode(false);
          SslHandler handler = new SslHandler(engine);
          handlers.put("ssl", handler);
        }

        // Do we need to deal with HTTP?
        if (http) {
          handlers.put("http-decoder", new HttpRequestDecoder());
          handlers.put("http-aggregator", new HttpChunkAggregator(65536));
          handlers.put("http-encoder", new HttpResponseEncoder());
          handlers.put("http-handler", new HttpAcceptorHandler(httpKeepAliveRunnable, 10000));
        }

        // The message decoder.
        handlers.put("message-decoder", new NettyMessageDecoder());

        // The channel handler.
        handlers.put("handler", new ServerChannelHandler(channelGroup));

        // Create and return the result.
        ChannelPipeline result = new StaticChannelPipeline(
            handlers.values().toArray(new ChannelHandler[handlers.size()]));
        return result;
      }
    };

    // Set the pipeline factory.
    bootstrap.setPipelineFactory(factory);

    bootstrap.setOption("child.tcpNoDelay", true);
//    bootstrap.setOption("child.receiveBufferSize", tcpReceiveBufferSize);
//    bootstrap.setOption("child.sendBufferSize", tcpSendBufferSize);
    bootstrap.setOption("reuseAddress", true);
    bootstrap.setOption("child.reuseAddress", true);
    bootstrap.setOption("child.keepAlive", true);

    // Setup the channel groups.
    channelGroup = new DefaultChannelGroup("NettyReceiver-Accepted");
    serverChannelGroup = new DefaultChannelGroup("NettyReceiver-Acceptor");

    // Bind to the specified port.
    bootstrap.bind(new InetSocketAddress(port));
  }

  @Override
  public void dispatch(List<ByteBuffer> buffers) {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void stop() {

    // Reel in any the keep alive runnable.
    if (httpKeepAliveRunnable != null) {
      httpKeepAliveRunnable.close();
    }

    // Close all the acceptors.
    LOG.info("Closing '{}' server channels", serverChannelGroup.size());
    ChannelGroupFuture future = serverChannelGroup.close().awaitUninterruptibly();
    if (!future.isCompleteSuccess()) {
      LOG.warn("Server channel group did not completely close");
      Iterator<Channel> iterator = future.getGroup().iterator();
      while (iterator.hasNext()) {
        Channel channel = iterator.next();
        if (channel.isBound()) {
          LOG.warn(String.format("Server channel '%s' is still connected to '%s'",
              channel, channel.getRemoteAddress()));
        }
      }
    }

    // Close all open channels.
    LOG.info("Closing '{}' channels", channelGroup.size());
    future = channelGroup.close().awaitUninterruptibly();
    if (!future.isCompleteSuccess()) {
      LOG.warn("Channel group did not completely close");
      Iterator<Channel> iterator = future.getGroup().iterator();
      while (iterator.hasNext()) {
        Channel channel = iterator.next();
        if (channel.isBound()) {
          LOG.warn(String.format("Channel '%s' is still connected to '%s'",
              channel, channel.getRemoteAddress()));
        }
      }
    }

    // Close down the channel factory.
    channelFactory.releaseExternalResources();
    channelFactory = null;
  }

  // Inner classes.

  protected class ServerChannelHandler extends SimpleChannelHandler {

    // Log.

    protected final Logger LOG = LoggerFactory.getLogger(ServerChannelHandler.class);

    // Instance fields.

    protected final ChannelGroup group;

    protected volatile boolean active;

    // Implementation.

    public ServerChannelHandler(ChannelGroup group) {
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
      byte[] buffer = (byte[]) e.getMessage();
      List<ByteBuffer> buffers = new ArrayList<ByteBuffer>(1);
      buffers.add(ByteBuffer.wrap(buffer));
      consumer.consume(buffers);
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

  protected static class HttpAcceptorHandler extends SimpleChannelHandler {

    // Instance fields.

    protected final BlockingQueue<ResponseHolder> responses =
        new LinkedBlockingQueue<ResponseHolder>();
    protected final BlockingQueue<Runnable> delayedResponses =
        new LinkedBlockingQueue<Runnable>();
    protected final Executor executor =
        new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS, delayedResponses);
    protected final HttpKeepAliveRunnable httpKeepAliveTask;
    protected final long responseTime;

    protected Channel channel;

    // Implementation.

    public HttpAcceptorHandler(HttpKeepAliveRunnable httpKeepAliveTask, long responseTime) {
      super();
      this.responseTime = responseTime;
      this.httpKeepAliveTask = httpKeepAliveTask;
    }

    public void keepAlive(long time) {

      // Send some responses to catch up thus avoiding any timeout.
      int lateResponses = 0;
      for (ResponseHolder response : responses) {
        if (response.timeReceived < time) {
          lateResponses++;
        } else {
          break;
        }
      }
      for (int i = 0; i < lateResponses; i++) {
        executor.execute(new ResponseRunner());
      }
    }

    // SimpleChannelHandler implementation.

    @Override
    public void channelConnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
      LOG.debug("Channel connected, ctx: '{}', event: {}", ctx.getName(), e);
      super.channelConnected(ctx, e);
      channel = e.getChannel();
      httpKeepAliveTask.registerKeepAliveHandler(this);
    }

    @Override
    public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e)
        throws Exception {
      LOG.debug("Channel disconnected, ctx: '{}', event: {}", ctx.getName(), e);
      super.channelDisconnected(ctx, e);
      httpKeepAliveTask.unregisterKeepAliveHandler(this);
      channel = null;
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
      LOG.debug("Message received, ctx: '{}', event: {}", ctx.getName(), e);

      HttpRequest request = (HttpRequest) e.getMessage();
      HttpMethod method = request.getMethod();

      // If we are a post then we send upstream, otherwise we are just being prompted for a response.
      if (method.equals(HttpMethod.POST)) {
        MessageEvent event = new UpstreamMessageEvent(e.getChannel(), request.getContent(), e.getRemoteAddress());
        ctx.sendUpstream(event);
      }

      // add a new response
      responses.put(new ResponseHolder(System.currentTimeMillis() + responseTime,
          new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)));
    }

    @Override
    public void writeRequested(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
      LOG.debug("Write requested, ctx: '{}', event: {}", ctx.getName(), e);

      // We are either a channel buffer, which gets delayed until a response is available,
      // or we are the actual response
      if (e.getMessage() instanceof ChannelBuffer) {
        ChannelBuffer buf = (ChannelBuffer) e.getMessage();
        executor.execute(new ResponseRunner(buf));
      } else {
        Channels.write(ctx, e.getFuture(), e.getMessage(), e.getRemoteAddress());
      }
    }

    // Inner classes.

    protected class ResponseRunner implements Runnable {
      private final ChannelBuffer buffer;

      private final boolean bogusResponse;

      public ResponseRunner(final ChannelBuffer buffer) {
        this.buffer = buffer;
        bogusResponse = false;
      }

      public ResponseRunner() {
        bogusResponse = true;
        buffer = ChannelBuffers.buffer(0);
      }

      public void run() {
        ResponseHolder responseHolder = null;
        do {
          try {
            responseHolder = responses.take();
          }
          catch (InterruptedException e) {
            // ignore, we'll just try again
          }
        }
        while (responseHolder == null);
        if (!bogusResponse) {
          ChannelBuffer piggyBackBuffer = piggyBackResponses();
          responseHolder.response.setContent(piggyBackBuffer);
          responseHolder.response.addHeader(HttpHeaders.Names.CONTENT_LENGTH,
              String.valueOf(piggyBackBuffer.writerIndex()));
          channel.write(responseHolder.response);
        } else {
          responseHolder.response.setContent(buffer);
          responseHolder.response.addHeader(HttpHeaders.Names.CONTENT_LENGTH, String.valueOf(buffer.writerIndex()));
          channel.write(responseHolder.response);
        }

      }

      private ChannelBuffer piggyBackResponses() {
        // if we are the last available response then we have to piggy back any remaining responses
        if (responses.isEmpty()) {
          ChannelBuffer buf = org.jboss.netty.buffer.ChannelBuffers.dynamicBuffer();
          buf.writeBytes(buffer);
          do {
            try {
              ResponseRunner responseRunner = (ResponseRunner) delayedResponses.poll(0, TimeUnit.MILLISECONDS);
              if (responseRunner == null) {
                break;
              }
              buf.writeBytes(responseRunner.buffer);
            }
            catch (InterruptedException e) {
              break;
            }
          }
          while (responses.isEmpty());
          return buf;
        }
        return buffer;
      }
    }

    protected static class ResponseHolder {
      final HttpResponse response;

      final long timeReceived;

      public ResponseHolder(final long timeReceived, final HttpResponse response) {
        this.timeReceived = timeReceived;
        this.response = response;
      }
    }

  }

  protected static class HttpKeepAliveRunnable implements Runnable {

    // Log.

    protected final Logger LOG = LoggerFactory.getLogger(HttpKeepAliveRunnable.class);

    // Instance fields.

    private final List<HttpAcceptorHandler> handlers = new ArrayList<HttpAcceptorHandler>();
    private boolean closed = false;
    private Future<?> future;

    // Implementation.

    public synchronized void registerKeepAliveHandler(HttpAcceptorHandler httpAcceptorHandler) {
      LOG.debug("Registering keep alive handler: '{}'", httpAcceptorHandler);
      handlers.add(httpAcceptorHandler);
    }

    public synchronized void unregisterKeepAliveHandler(HttpAcceptorHandler httpAcceptorHandler) {
      LOG.debug("Unregistering keep alive handler: '{}'", httpAcceptorHandler);
      handlers.remove(httpAcceptorHandler);
    }

    public void close() {
      LOG.debug("Closing keep alive handler");
      if (future != null) {
        future.cancel(false);
      }
      closed = true;
    }

    public synchronized void setFuture(final Future<?> future) {
      this.future = future;
    }

    // Runnable implementation.

    public synchronized void run() {

      // Bail if we have been already closed.
      if (closed) {
        return;
      }

      // Do the keep alive.
      LOG.debug("Keep alive for '{}' handlers", handlers.size());
      long time = System.currentTimeMillis();
      for (HttpAcceptorHandler handler : handlers) {
        handler.keepAlive(time);
      }
    }
  }
}

