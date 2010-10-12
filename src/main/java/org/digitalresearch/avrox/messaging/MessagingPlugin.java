package org.digitalresearch.avrox.messaging;

/**
 * Like RPCPlugin, but simpler :)
 *
 * @author Christian (raychaser@gmail.com)
 */
public interface MessagingPlugin {

  // Interface.

  /**
   * Called right before the message is transmitted.
   *
   * @param context The messaging context.
   */
  void messageSending(MessagingContext context);

  /**
   * Called right after the message has been received.
   *
   * @param context The messaging context.
   */
  void messageReceiving(MessagingContext context);
}
