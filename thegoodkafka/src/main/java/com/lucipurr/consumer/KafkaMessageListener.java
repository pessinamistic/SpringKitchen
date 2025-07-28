/* (C) Lucipurr 69@420 */
package com.lucipurr.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/*
 * All the listener class should implement this interface.
 */
public interface KafkaMessageListener {

  void onMessage(ConsumerRecord<Object, Object> message);

  /**
   * SPECIAL HANDLER FOR CONSUMER EXCEPTION there could be multiple exceptions in consumer that
   * could left consumer thread hanging and doing nothing, this default method is to handle those on
   * need basis
   *
   * @param consumer to take action on/from consumer object
   * @param exception to take action on/from the type of exception raised
   * @return boolean to tell whether on not consumer thread to close
   */
  default boolean isConsumerCloseRequiredForCurrentConsumerState(
      KafkaConsumer<Object, Object> consumer, Exception exception) {
    return false;
  }
}
