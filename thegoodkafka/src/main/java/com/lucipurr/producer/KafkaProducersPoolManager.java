/* (C) Lucipurr 69@420 */
package com.lucipurr.producer;

import jakarta.annotation.PreDestroy;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class KafkaProducersPoolManager {

  private final Map<String, KafkaMessageProducer> kafkaMessageProducerMap = new HashMap<>();

  public KafkaMessageProducer createKafkaMessageProducer(String className) {
    KafkaMessageProducer kafkaMessageProducer = new KafkaMessageProducer();
    addKafkaProducersToMap(className, kafkaMessageProducer);

    return kafkaMessageProducer;
  }

  private void addKafkaProducersToMap(String className, KafkaMessageProducer kafkaMessageProducer) {
    log.debug("Adding Kafka Producer Pool to map of class:: {}", className);
    kafkaMessageProducerMap.put(className, kafkaMessageProducer);
  }

  public KafkaMessageProducer getKafkaMessageProducer(String className) {
    return kafkaMessageProducerMap.get(className) != null
        ? kafkaMessageProducerMap.get(className)
        : null;
  }

  @PreDestroy
  public void shutDownKafkaMessageProducerPoolGracefully() {
    for (KafkaMessageProducer kafkaMessageProducer : kafkaMessageProducerMap.values()) {
      kafkaMessageProducer.shutDownGracefully();
    }

    kafkaMessageProducerMap.clear();
  }
}
