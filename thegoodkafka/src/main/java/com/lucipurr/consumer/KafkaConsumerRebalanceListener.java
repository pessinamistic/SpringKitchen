/* (C) Lucipurr 69@420 */
package com.lucipurr.consumer;

import java.util.Collection;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

@Slf4j
@Setter
@Getter
public class KafkaConsumerRebalanceListener implements ConsumerRebalanceListener {

  private KafkaConsumer<Object, Object> consumer;

  @Override
  public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
    log.info("onPartitionsRevoked");
  }

  @Override
  public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    log.info("onPartitionsAssigned");
  }
}
