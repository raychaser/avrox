package org.digitalresearch.avrox.messaging;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import javax.net.ssl.SSLEngine;

import org.jboss.netty.bootstrap.*;
import org.jboss.netty.buffer.*;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.nio.*;
import org.jboss.netty.handler.codec.http.*;
import org.jboss.netty.handler.ssl.*;
import org.slf4j.*;

import static org.jboss.netty.buffer.ChannelBuffers.*;
import static org.jboss.netty.channel.Channels.*;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.*;
import static org.jboss.netty.handler.codec.http.HttpHeaders.*;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.*;
import static org.jboss.netty.handler.codec.http.HttpVersion.*;


/**
 * A Receiver that binds to a transport based on the HTTP protocol.
 *
 * @author Christian (raychaser@gmail.com)
 */
@SuppressWarnings("PMD.TooManyStaticImports")
public class HTTPReceiver implements TransmitterPiggybackReceiver {

  // Log.

  protected static final Logger LOG = LoggerFactory.getLogger(HTTPReceiver.class);

  // Instance fields.

  protected final Consumer consumer;
  protected final String host;
  protected final int port;
  protected final boolean compress;
  protected final boolean ssl;
  protected final ExecutorService consumerExecutor;
  protected final LinkedBlockingQueue<List<ByteBuffer>> responseMessageQueue =
      new LinkedBlockingQueue<List<ByteBuffer>>(100);

  protected ServerBootstrap bootstrap;

  // Implementation.

  public HTTPReceiver(Consumer consumer, String host, int port, boolean compress, boolean ssl) {
    this.consumer = consumer;
    this.host = host;
    this.port = port;
    this.compress = compress;
    this.ssl = ssl;
    consumerExecutor = createConsumerPool();
  }

  protected SSLEngine createSSLEngine() {
    SSLEngine result = DefaultSSLContextFactory.getServerContext().createSSLEngine();
    result.setUseClientMode(false);
    return result;
  }

  protected ExecutorService createBossPool() {
    // Consider overriding in subclass, this pool is unbounded!
    ExecutorService result = Executors.newCachedThreadPool();
    return result;
  }

  protected ExecutorService createWorkPool() {
    // Consider overriding in subclass, this pool is unbounded!
    ExecutorService result = Executors.newCachedThreadPool();
    return result;
  }

  protected ExecutorService createConsumerPool() {
    // Consider overriding in subclass, this pool is unbounded!
    ExecutorService result = Executors.newCachedThreadPool();
    return result;
  }

  protected int getAggregatorBytes() {
    return 32 * 1024 * 1024;
  }

  // TransmitterPiggybackReceiver implementation.

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

    // Configure the server.
    bootstrap = new ServerBootstrap(
        new NioServerSocketChannelFactory(createBossPool(), createWorkPool()));

    // Set up the event pipeline factory.
    bootstrap.setPipelineFactory(new HttpServerPipelineFactory());

    // Bind and start to accept incoming connections.
    bootstrap.bind(new InetSocketAddress(port));
  }

  @Override
  public void dispatch(final List<ByteBuffer> buffers) {
    consumerExecutor.submit(new Runnable() {
      @Override
      public void run() {
        try {
          consumer.consume(buffers);
        } catch (Throwable t) {
          LOG.error("Error consuming", t);
        }
      }
    });
  }

  @Override
  public void stop() {

    // Shutdown the server.
    bootstrap.releaseExternalResources();
  }

  // Inner classes.

  public class HttpServerPipelineFactory implements ChannelPipelineFactory {

    public ChannelPipeline getPipeline() throws Exception {

      // Create a default pipeline implementation.
      ChannelPipeline pipeline = pipeline();

      // Enable SSL if requested.
      if (ssl) {
        SSLEngine engine = createSSLEngine();
        pipeline.addLast("ssl", new SslHandler(engine));
      }

      // Add to the pipeline.
      pipeline.addLast("decoder", new HttpRequestDecoder());
      pipeline.addLast("inflater", new HttpContentDecompressor());
      pipeline.addLast("aggregator", new HttpChunkAggregator(getAggregatorBytes()));
      pipeline.addLast("encoder", new HttpResponseEncoder());

      // Response compression, if so desired.
      if (compress) pipeline.addLast("deflater", new HttpContentCompressor());

      pipeline.addLast("handler", new HttpRequestHandler());
      return pipeline;
    }
  }

  public class HttpRequestHandler extends SimpleChannelUpstreamHandler {

    // Implementation.

    protected void writeResponse(MessageEvent e, HttpRequest request) {

      // Build the response object.
      HttpResponse response = new DefaultHttpResponse(HTTP_1_1, OK);

      // Decide whether to close the connection or not.
      boolean keepAlive = isKeepAlive(request);
      if (keepAlive) {

        // Add 'Content-Length' header only for a keep-alive connection.
        response.setHeader(CONTENT_LENGTH, response.getContent().readableBytes());
      }

      // Encode the queued messages into the response.
      List<List<ByteBuffer>> responseBuffers =
          new ArrayList<List<ByteBuffer>>(responseMessageQueue.size());
      responseMessageQueue.drainTo(responseBuffers);
      ChannelBuffer[] channelBuffers = new ChannelBuffer[responseBuffers.size() * 2 + 1];
      int index = 0;
      channelBuffers[index] = buffer(4);
      channelBuffers[index].writeInt(responseBuffers.size());
      for (int i = 0; i < responseBuffers.size(); i++) {
        List<ByteBuffer> buffers = responseBuffers.get(i);
        int buffersLength = 0;
        ChannelBuffer[] messageBuffers = new ChannelBuffer[buffers.size()];
        for (int j = 0; j < buffers.size(); j++) {
          buffersLength += buffers.get(j).remaining();
          messageBuffers[j] = wrappedBuffer(buffers.get(j));
        }
        channelBuffers[++index] = buffer(4);
        channelBuffers[index].writeInt(buffersLength);
        channelBuffers[++index] = wrappedBuffer(messageBuffers);
      }
      ChannelBuffer content = wrappedBuffer(channelBuffers);

//      // Compression requested?
//      if (compress) {
//
//        // Encode the request content using ZLib deflate. Technically, it
//        // should be possible to accomplish this using the pipeline, but
//        // i was so far unable to do that.
//        EncoderEmbedder encoder = new EncoderEmbedder(new ZlibEncoder());
//        encoder.offer(content);
//        content = (ChannelBuffer) encoder.poll();
//        encoder.finish();
//        request.setHeader(CONTENT_ENCODING, DEFLATE);
//      }

      response.setContent(content);
      HttpHeaders.setContentLength(response, content.readableBytes());

      // Write the response.
      ChannelFuture future = e.getChannel().write(response);

      // Close the non-keep-alive connection after the write operation is done.
      if (!keepAlive) future.addListener(ChannelFutureListener.CLOSE);
    }

    // SimpleChannelUpstreamHandler implementation.

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {

      // Get the request.
      HttpRequest request = null;

      try {

        // Get the request.
        request = (HttpRequest) e.getMessage();

        // Get the content...
        ChannelBuffer content = request.getContent();
        List<ByteBuffer> buffers = new ArrayList<ByteBuffer>(1);
        buffers.add(content.toByteBuffer());

        // ...and dispatch it.
        dispatch(buffers);

      } catch (Throwable t) {

        // Log some info on the error.
        LOG.error("Error getting content or dispatching message", t);

      } finally {

        // Write the response.
        writeResponse(e, request); // TODO: Communicate errors.
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
      e.getCause().printStackTrace();
      e.getChannel().close();
    }
  }
}
