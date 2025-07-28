/* (C) Lucipurr 69@420 */
package com.lucipurr.consumer;

import jakarta.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Kafka Consumer Pool.
 *
 * All the consumers will be created in here.
 *
 * For consuming the messages , the module has to  pass the listener class.
 * Which has to implement the KafkaMessageListener interface.
 *
 */
public class KafkaConsumersPool {

  private static final Logger log = LoggerFactory.getLogger(KafkaConsumersPool.class);

  private final List<KafkaMessagesConsumer> kafkaMessagesConsumerList = new ArrayList<>();
  private ExecutorService executorService;

  //  public void initialiseKafkaConsumers(
  //      KafkaMessageListener kafkaMessageListener, KafkaConfig kafkaConfiguration) throws
  // Exception {
  //    Properties properties = kafkaConfiguration.initConsumerProperties();
  //    int consumerPoolSize = (int) properties.get("consumer.pool.size");
  //    log.debug("Creating and adding consumers with default configuration: {}", consumerPoolSize);
  //
  //    for (int i = 0; i < consumerPoolSize; i++) {
  //      kafkaMessagesConsumerList.add(new KafkaMessagesConsumer(properties,
  // kafkaMessageListener));
  //    }
  //  }

  public void initialiseKafkaConsumers(
      KafkaMessageListener kafkaMessageListener, Properties properties) {
    int consumerPoolSize = (int) properties.get("consumer.pool.size");
    log.debug(
        "Creating and adding consumers with user specified configuration: {}", consumerPoolSize);

    for (int i = 0; i < consumerPoolSize; i++) {
      kafkaMessagesConsumerList.add(new KafkaMessagesConsumer(properties, kafkaMessageListener));
    }
  }

  public void initialiseKafkaConsumers(
      KafkaMessageListener kafkaMessageListener,
      Properties properties,
      List<KafkaConsumerRebalanceListener> consumerRebalanceListeners) {
    int consumerPoolSize = (int) properties.get("consumer.pool.size");
    log.debug(
        "Creating and adding kafka consumers with user specified configuration and custom rebalancers: {}",
        consumerPoolSize);

    for (int i = 0; i < consumerPoolSize; i++) {
      kafkaMessagesConsumerList.add(
          new KafkaMessagesConsumer(
              properties, kafkaMessageListener, consumerRebalanceListeners.get(i)));
    }
  }

  public void initialiseKafkaConsumers(
      KafkaAllMessagesListener kafkaAllMessagesListener,
      Properties properties,
      List<KafkaConsumerRebalanceListener> consumerRebalanceListeners) {
    int consumerPoolSize = (int) properties.get("consumer.pool.size");
    log.debug(
        "Creating and adding kafka consumers for all messages with user specified configuration and custom rebalancers: {}",
        consumerPoolSize);

    for (int i = 0; i < consumerPoolSize; i++) {
      kafkaMessagesConsumerList.add(
          new KafkaMessagesConsumer(
              properties, kafkaAllMessagesListener, consumerRebalanceListeners.get(i)));
    }
  }

  /*
   * This method be initialized for consuming the message from Kafka broker.
   */
  @SuppressWarnings("unused")
  public void startConsuming() {
    executorService = Executors.newFixedThreadPool(kafkaMessagesConsumerList.size());
    log.debug("Pool Size::{}", kafkaMessagesConsumerList.size());
    for (KafkaMessagesConsumer kafkaMessagesConsumer : kafkaMessagesConsumerList) {
      try {
        executorService.execute(kafkaMessagesConsumer);
      } catch (Exception e) {
        log.error(
            "Cannot submit kafka message consumer for topic [{}] to executor pool. Consumer parameters [{}]",
            kafkaMessagesConsumer.topic,
            kafkaMessagesConsumer.props,
            e);
      }
    }

    Runtime.getRuntime().addShutdownHook(new Thread(this::shutDownGracefully));
  }

  public void shutDownGracefully() {
    log.info("Starting shutdown of [{}] kafka consumers", kafkaMessagesConsumerList.size());
    try {
      // Sleep a second, and then interrupt
      try {
        Thread.sleep(1000);

        for (KafkaMessagesConsumer reader : kafkaMessagesConsumerList) {
          reader.setPrepareExit(true);
        }
        for (KafkaMessagesConsumer consumer : kafkaMessagesConsumerList) {
          log.info(
              "Trying to gracefully shut down kafka messages consumer for topic [{}], with properties [{}]",
              consumer.topic,
              consumer.props);
          consumer.shutDownGracefully();
        }
      } catch (InterruptedException e) {
        log.error("Cannot shut down kafka consumers gracefully", e);
      }
      executorService.shutdown();
      boolean ignored = executorService.awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      log.error("Exception while awaitTermination of kafka consumers executor", e);
    }
    log.info("Kafka consumers were shut down. Process can be safely terminated...");
  }

  @SuppressWarnings("unused")
  public ExecutorService getExecutorService() {
    return executorService;
  }

  @PreDestroy
  void closeConsumers() {
    shutDownGracefully();
  }
}
