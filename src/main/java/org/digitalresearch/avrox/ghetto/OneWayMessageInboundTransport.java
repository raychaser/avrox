package org.digitalresearch.avrox.ghetto;

/**
 * Interface for implementations of the transmitting end of a one-way
 * transport, such as a message queue.
 *
 * @author Christian (raychaser@gmail.com)
 */
public interface OneWayMessageInboundTransport {

  // Interface.

  /**
   * For every incoming message, also notify this processor.
   *
   * @param messageProcessor A processor to be notified of all incoming
   *                         messages.
   */
  void addProcessor(OneWayMessageProcessor messageProcessor);

  /**
   * Start consuming. Hook to initialize whatever the desired transport
   * requires to start receiving messages.
   */
  void start();
}
