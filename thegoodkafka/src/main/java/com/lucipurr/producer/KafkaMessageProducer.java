/* (C) Lucipurr 69@420 */
package com.lucipurr.producer;

import com.lucipurr.exception.PublisherException;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;

@Slf4j
@Getter
public class KafkaMessageProducer {

  private Producer<Object, Object> producer;

  //  public void init(KafkaConfig kafkaBasic) {
  //    producer = new KafkaProducer<>(kafkaBasic.initProducerProperties());
  //    log.info("Kafka producer initialized");
  //  }
  //
  //  public void initConfigurableProducer(Properties properties) {
  //    producer = new KafkaProducer<>(properties);
  //    log.info("Kafka producer initialized with user specified properties");
  //  }

  public Future<RecordMetadata> publishKafkaMessage(String topic, Object message) {
    Future<RecordMetadata> future =
        producer.send(new ProducerRecord<>(topic, message), new ProducerCallback(""));
    log.info("Published kafka message [{}] on topic [{}]", message, topic);
    return future;
  }

  public Future<RecordMetadata> publishKafkaMessage(
      String topic, Object message, Callback callback) {
    Future<RecordMetadata> future = producer.send(new ProducerRecord<>(topic, message), callback);
    log.info("Published kafka message [{}] with custom callback on topic [{}]", message, topic);
    return future;
  }

  public Future<RecordMetadata> publishKafkaMessage(String topic, Object key, Object message) {
    Future<RecordMetadata> future =
        producer.send(new ProducerRecord<>(topic, key, message), new ProducerCallback(""));
    log.info("Published kafka message [{}] on topic [{}], key [{}]", message, topic, key);
    return future;
  }

  /**
   * This method can be used to send kafka message with custom headers (such headers can contain any
   * metadata not directly related to the business logic contained in message). Non-mandatory
   * parameters are marked as (nullable). Please refere to docs in {@link
   * org.apache.kafka.clients.producer.ProducerRecord#ProducerRecord(String, Integer, Object,
   * Object, Iterable)} for further description for each of the parameters.
   *
   * @param topic
   * @param partition (nullable)
   * @param key (nullable)
   * @param message
   * @param headers (nullable)
   * @return
   */
  public Future<RecordMetadata> publishKafkaMessage(
      String topic, Integer partition, Object key, Object message, RecordHeader... headers) {
    Iterable<Header> headersList = (headers != null) ? Arrays.asList(headers) : null;
    Future<RecordMetadata> future =
        producer.send(
            new ProducerRecord<>(topic, partition, key, message, headersList),
            new ProducerCallback(""));
    log.info(
        "Published kafka message [{}] on topic [{}], partition [{}], key [{}]",
        message,
        topic,
        partition,
        key);
    return future;
  }

  public Future<RecordMetadata> publishKafkaMessage(
      String topic, Object key, Object message, Callback callback) {
    Future<RecordMetadata> future =
        producer.send(new ProducerRecord<>(topic, key, message), callback);
    log.info(
        "Published kafka message [{}] with custom callback on topic [{}], key [{}]",
        message,
        topic,
        key);
    return future;
  }

  public RecordMetadata PublishGenericRecordKafka(String topic, Object message)
      throws InterruptedException, ExecutionException {
    RecordMetadata recordMetadata =
        producer.send(new ProducerRecord<>(topic, message), new ProducerCallback("")).get();
    log.info("Synchronously published kafka message [{}] on topic [{}]", message, topic);
    return recordMetadata;
  }

  public static class ProducerCallback implements Callback {
    protected final String message;

    public ProducerCallback(String message) {
      this.message = message;
    }

    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
      if (e != null) {
        log.info("Failed to send kafka message: {}", message);
        throw new PublisherException(e.getMessage());
      }
      log.info("Completed kafka message dispatch");
    }
  }

  public static final class ProducerCallbackError extends ProducerCallback {
    public ProducerCallbackError(String message) {
      super(message);
    }

    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
      if (e != null) {
        log.error("Failed to send kafka message: {}", message);
        throw new PublisherException(e.getMessage());
      }
      log.info("Completed kafka message dispatch");
    }
  }

  public void shutDownGracefully() {
    log.info("Closing kafka publisher...");
    producer.close();
    log.info("Successfully closed kafka publisher");
  }
}
