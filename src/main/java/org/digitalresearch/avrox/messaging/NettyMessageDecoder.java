package org.digitalresearch.avrox.messaging;

import java.security.MessageDigest;

import org.jboss.netty.buffer.*;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.compression.*;
import org.jboss.netty.handler.codec.embedder.*;
import org.jboss.netty.handler.codec.frame.*;

/**
 * @author Christian (raychaser@gmail.com)
 */
public class NettyMessageDecoder extends FrameDecoder {

  @Override
  protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer)
      throws Exception {

    // Wait until we have received 10 bytes - the magic number,
    // the version and content headers, and a length int.
    if (buffer.readableBytes() < 10) {
      return null;
    }

    // Check the magic number.
    buffer.markReaderIndex();
    int magicNumber = buffer.readUnsignedByte();
    if (magicNumber != 'F') {
      buffer.resetReaderIndex();
      throw new CorruptedFrameException(
          String.format("Invalid magic number: '%d'", magicNumber));
    }

    // Read version information.
    int versionTag = buffer.readUnsignedByte();
    if (versionTag != 'V') {
      buffer.resetReaderIndex();
      throw new CorruptedFrameException(
          String.format("Expected version tag but got: '%d'", versionTag));
    }
    int majorVersion = buffer.readUnsignedByte();
    if (majorVersion != 1) {
      buffer.resetReaderIndex();
      throw new CorruptedFrameException(
          String.format("Expected major version 1 but got: '%d'", majorVersion));
    }
    int minorVersion = buffer.readUnsignedByte();
    if (minorVersion != 1) {
      buffer.resetReaderIndex();
      throw new CorruptedFrameException(
          String.format("Expected minor version 1 but got: '%d'", minorVersion));
    }

    // Read content information.
    int contentTag = buffer.readUnsignedByte();
    if (contentTag != 'C') {
      buffer.resetReaderIndex();
      throw new CorruptedFrameException(
          String.format("Expected content tag but got: '%d'", contentTag));
    }
    int contentEncoding = buffer.readUnsignedByte();
    if (contentEncoding < 0 || contentEncoding > 1) {
      buffer.resetReaderIndex();
      throw new CorruptedFrameException(
          String.format("Expected content encoding 0 or 1 but got: '%d'", contentEncoding));
    }
    boolean decompress = false;
    if (contentEncoding == 1) {
      decompress = true;
    }

    // Read digest information.
    int digestTag = buffer.readUnsignedByte();
    if (digestTag != 'D') {
      buffer.resetReaderIndex();
      throw new CorruptedFrameException(
          String.format("Expected digest tag but got: '%d'", contentTag));
    }
    ChannelBuffer digest = buffer.readBytes(16);

    // Wait until the whole data is available.
    int length = buffer.readInt();
    if (buffer.readableBytes() < length) {
      System.err.printf("Needed: '%d', got only: '%d'\n",
          length, buffer.readableBytes());
      buffer.resetReaderIndex();
      return null;
    }

    // Decompress if needed.
    if (decompress) {
      DecoderEmbedder decoder = new DecoderEmbedder(new ZlibDecoder());
      decoder.offer(buffer);
      buffer = (ChannelBuffer) decoder.poll();
      decoder.finish();
//      System.err.printf("Before decompress: '%d', after: '%d', ratio: '%.2f'\n",
//          length, buffer.readableBytes(), length / (double) buffer.readableBytes());
    }

    // Read the message data and verify the digest.
    byte[] result = new byte[buffer.readableBytes()];
    buffer.readBytes(result);
    MessageDigest m = MessageDigest.getInstance("MD5");
    byte[] computedDigest = m.digest(result);
    for (int i = 0; i < computedDigest.length; i++) {
      if (computedDigest[i] != digest.readByte()) {
        throw new RuntimeException("Digest mismatch - computed: '-s' but should be '-s'");
      }
    }

    // Return.
    return result;
  }
}
