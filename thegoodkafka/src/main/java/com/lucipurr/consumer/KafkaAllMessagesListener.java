/* (C) Lucipurr 69@420 */
package com.lucipurr.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public interface KafkaAllMessagesListener {

  void onMessage(ConsumerRecords<Object, Object> records);

  /**
   * Default method to provide a way to recover a consumer in case of recoverable exceptions. In
   * case anyone needs to add specific handling for an exception, they can override this method.
   *
   * @param consumer - Kafka Consumer
   * @param exception - Exception
   * @return boolean - true if consumer close is required, false otherwise
   */
  default boolean isConsumerCloseRequiredForCurrentConsumerState(
      KafkaConsumer<Object, Object> consumer, Exception exception) {
    return false;
  }
}
