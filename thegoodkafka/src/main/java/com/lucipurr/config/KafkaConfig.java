/* (C) Lucipurr 69@420 */
package com.lucipurr.config;

import com.lucipurr.consumer.KafkaConsumerGroupUtility;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/*
 *
 * Configuration for Kafka
 *
 */
@Slf4j
@Component
public class KafkaConfig {

  private static final String AVAILABLE_KAFKA_CG_KEY = "default.available.kafka.consumer.groups";
  private static final int ANY_APP_INSTANCE_CONSUMES = 1;
  private static final int ALL_APP_INSTANCE_CONSUMES = 2;

  // @Autowired private Configuration config;

  @Autowired private KafkaConsumerGroupUtility kafkaConsumerGroupUtility;

  private Properties propsConsumer;
  private Properties propsPublisher;

  //  public Properties initConsumerProperties() throws Exception {
  //    propsConsumer = new Properties();
  //    String consumerGroupID = null;
  //
  //    propsConsumer.put(
  //        "bootstrap.servers", config.get("kafka.bootstrap.cons.servers", "localhost:9092"));
  //
  //    if (config.get("consumer.instance.consumption.type") == null
  //        || ("").equals(config.get("consumer.instance.consumption.type"))) {
  //      String message =
  //          "Kafka consumer instance consumption type property in ZK is not present. Can't create
  // consumers.";
  //      log.error(message);
  //      throw new Exception(message);
  //    }
  //
  //    switch (config.getInt("consumer.instance.consumption.type")) {
  //        /*ANY_APP_INSTANCE_CONSUMES -> Gives the capability that
  //         * any running instance of service should consume the message*/
  //      case ANY_APP_INSTANCE_CONSUMES:
  //        consumerGroupID = config.get("kafka.default.group.id");
  //        break;
  //
  //        /*ALL_APP_INSTANCE_CONSUMES -> Gives the capability that
  //         * each running instance of service should consume the message*/
  //      case ALL_APP_INSTANCE_CONSUMES:
  //        if (consumerGroupID == null || consumerGroupID.equals("")) {
  //          if (config.get(AVAILABLE_KAFKA_CG_KEY) == null
  //              || config.get(AVAILABLE_KAFKA_CG_KEY).equals(""))
  //            consumerGroupID = UUID.randomUUID().toString();
  //          else
  //            consumerGroupID =
  // kafkaConsumerGroupUtility.fetchConsumerGroup(AVAILABLE_KAFKA_CG_KEY);
  //        }
  //        break;
  //    }
  //
  //    propsConsumer.put("group.id", consumerGroupID);
  //    propsConsumer.put(
  //        "enable.auto.commit", config.get("kafka.consumer.enable.auto.commit", "false"));
  //    propsConsumer.put(
  //        "key.deserializer",
  //        config.get(
  //            "kafka.consumer.key.deserializer",
  //            "org.apache.kafka.common.serialization.StringDeserializer"));
  //    propsConsumer.put(
  //        "value.deserializer",
  //        config.get(
  //            "kafka.consumer.value.deserializer",
  //            "org.apache.kafka.common.serialization.StringDeserializer"));
  //    propsConsumer.put("fetch.min.bytes", config.get("kafka.fetch.min.bytes", "300000"));
  //    propsConsumer.put("max.poll.records", config.get("kafka.max.poll.records", "500"));
  //    propsConsumer.put("fetch.max.wait.ms", config.get("kafka.fetch.max.wait.ms", "5000"));
  //    propsConsumer.put(
  //        "max.partition.fetch.bytes", config.get("kafka.max.partition.fetch.bytes", "1048576"));
  //    propsConsumer.put("auto.offset.reset", config.get("kafka.auto.offset.reset", "latest"));
  //    propsConsumer.put("session.timeout.ms", config.get("kafka.session.timeout.ms", "180000"));
  //    propsConsumer.put("request.timeout.ms", config.get("kafka.request.timeout.ms", "180001"));
  //    propsConsumer.put("heartbeat.interval.ms", config.get("kafka.heartbeat.interval.ms",
  // "11000"));
  //    propsConsumer.put("metadata.max.age.ms", config.getInt("kafka.metadata.max.age.ms", 60000));
  //    propsConsumer.put("max.poll.interval.ms", config.getInt("kafka.max.poll.interval.ms",
  // 300000));
  //    propsConsumer.put("consumer.poll.time", config.getLong("kafka.poll.time", 100l));
  //    propsConsumer.put("consumer.topic", config.get("kafka.consumer.topic", "sampletopic"));
  //    propsConsumer.put("consumer.pool.size", config.getInt("kafka.consumer.pool.size"));
  //    propsConsumer.put("consumer.commit.option", config.getInt("kafka.consumer.commit.option"));
  //
  //    log.info("Kafka consumer properties initialized to: [{}]", propsConsumer);
  //
  //    return propsConsumer;
  //  }
  //
  //  public Properties initProducerProperties() {
  //    propsPublisher = new Properties();
  //
  //    propsPublisher.put(
  //        "bootstrap.servers", config.get("kafka.bootstrap.servers", "localhost:9092"));
  //    propsPublisher.put("acks", config.get("kafka.producer.acks", "1"));
  //    propsPublisher.put("retries", 2); // total 3 tries. retry interval default is 100
  //    // ms and can be configured using
  //    // retry.backoff.ms
  //    // producer will send the batch once batch.size is full or linger.ms is
  //    // reached for the batch
  //    propsPublisher.put(
  //        "batch.size", Integer.valueOf(config.get("kafka.producer.batch.size", "16384")));
  //    propsPublisher.put("linger.ms", Integer.valueOf(config.get("kafka.producer.linger.ms",
  // "300")));
  //    propsPublisher.put("block.on.buffer.full", config.get("kafka.block.on.buffer.full",
  // "false"));
  //    // buffer.memory: The total bytes of memory the
  //    // producer can use to buffer records waiting to be sent to the server.
  //    // comes into
  //    // picture when msgs are produced at a higher rate than can be delivered
  //    // to broker. Should be less than the heap allocation.
  //    propsPublisher.put(
  //        "buffer.memory", Integer.valueOf(config.get("kafka.producer.buffer.memory",
  // "33554432")));
  //    propsPublisher.put("compression.type", config.get("kafka.producer.compression.type",
  // "gzip"));
  //    propsPublisher.put(
  //        "key.serializer",
  //        config.get(
  //            "kafka.producer.key.serializer",
  //            "org.apache.kafka.common.serialization.StringSerializer"));
  //    propsPublisher.put(
  //        "value.serializer",
  //        config.get(
  //            "kafka.producer.value.serializer",
  //            "org.apache.kafka.common.serialization.StringSerializer"));
  //
  //    log.info("Kafka producer properties initialized to: [{}]", propsPublisher);
  //
  //    return propsPublisher;
  //  }
}
