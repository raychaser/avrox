package org.digitalresearch.avrox.messaging;

/**
 * A transmitter that can act as a receiver.
 *
 * @author Christian (raychaser@gmail.com)
 */
public interface ReceiverPiggybackTransmitter extends Transmitter {

  // Interface.

  void addResponseMessageReceiver(Receiver responseMessageReceiver);
}
