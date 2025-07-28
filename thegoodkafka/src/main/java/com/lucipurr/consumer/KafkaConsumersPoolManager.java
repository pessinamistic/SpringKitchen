/* (C) Lucipurr 69@420 */
package com.lucipurr.consumer;

import com.lucipurr.config.KafkaConfig;
import jakarta.annotation.PreDestroy;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class KafkaConsumersPoolManager {

  private static final Logger log = LoggerFactory.getLogger(KafkaConsumersPoolManager.class);

  @Autowired private KafkaConfig kafkaConfig;

  private final Map<String, KafkaConsumersPool> kafkaConsumerPoolMap = new HashMap<>();

  /**
   * Create Kafka Consumer Pool & Initializes consumer with user specified configuration. Also, here
   * we can pass list of Consumer Rebalance Listeners to get callback whenever consumer rebalance
   * happens & accordingly execute custom application logic.
   *
   * @param className - Class for which consumer pool is generated
   * @param kafkaListener - Listener class to handle consumed messages
   * @param propsConsumer - User specified kafka consumer properties
   * @param consumerRebalanceListeners - List<KafkaConsumerRebalanceListener>
   * @return Pool of Kafka Consumers - KafkaConsumersPool
   */
  @SuppressWarnings("unused")
  public KafkaConsumersPool createKafkaConsumerPool(
      String className,
      KafkaMessageListener kafkaListener,
      Properties propsConsumer,
      List<KafkaConsumerRebalanceListener> consumerRebalanceListeners) {
    KafkaConsumersPool kafkaConsumersPool = new KafkaConsumersPool();

    log.debug("Adding Kafka Consumer Pool to map");

    if (kafkaConsumerPoolMap.get(className) != null) {
      log.debug("Consumer pool already exists for class::" + className);
      return kafkaConsumerPoolMap.get(className);
    }

    kafkaConsumerPoolMap.put(className, kafkaConsumersPool);

    try {
      kafkaConsumersPool.initialiseKafkaConsumers(
          kafkaListener, propsConsumer, consumerRebalanceListeners);
    } catch (Exception ex) {
      log.error(
          "Error while initialising Kafka Consumer:: {}, Exception:: {}", ex.getMessage(), ex);
      shutDownKafkaConsumersFactoryPoolGracefully();
    }

    return kafkaConsumersPool;
  }

  /**
   * Create Kafka Consumer Pool & Initializes consumer with user specified configuration. Also, here
   * we can pass list of Consumer Rebalance Listeners to get callback whenever consumer rebalance
   * happens & accordingly execute custom application logic.
   *
   * @param className - Class for which consumer pool is generated
   * @param kafkaAllMessagesListener - Listener class to handle all consumed messages in one poll
   *     cycle at once
   * @param propsConsumer - User specified kafka consumer properties
   * @param consumerRebalanceListeners - List<KafkaConsumerRebalanceListener>
   * @return Pool of Kafka Consumers - KafkaConsumersPool
   */
  @SuppressWarnings("unused")
  public KafkaConsumersPool createKafkaConsumerPool(
      String className,
      KafkaAllMessagesListener kafkaAllMessagesListener,
      Properties propsConsumer,
      List<KafkaConsumerRebalanceListener> consumerRebalanceListeners) {
    KafkaConsumersPool kafkaConsumersPool = new KafkaConsumersPool();

    log.debug("Adding Kafka Consumer Pool to map");

    if (kafkaConsumerPoolMap.get(className) != null) {
      log.debug("Consumer pool already exists for class::" + className);
      return kafkaConsumerPoolMap.get(className);
    }

    kafkaConsumerPoolMap.put(className, kafkaConsumersPool);

    try {
      kafkaConsumersPool.initialiseKafkaConsumers(
          kafkaAllMessagesListener, propsConsumer, consumerRebalanceListeners);
    } catch (Exception ex) {
      log.error(
          "Error while initialising Kafka Consumer:: {}, Exception:: {}", ex.getMessage(), ex);
      shutDownKafkaConsumersFactoryPoolGracefully();
    }

    return kafkaConsumersPool;
  }

  /**
   * Create Kafka Consumer Pool & Initializes consumer with user specified configuration
   *
   * @param className - Class for which consumer pool is generated
   * @param kafkaListener - Listener class to handle consumed messages
   * @param propsConsumer - User specified kafka consumer properties
   * @return Created Kafka Consumers Pool
   */
  @SuppressWarnings("unused")
  public KafkaConsumersPool createKafkaConsumerPool(
      String className, KafkaMessageListener kafkaListener, Properties propsConsumer) {
    KafkaConsumersPool kafkaConsumersPool = new KafkaConsumersPool();

    log.debug("Adding Kafka Consumer Pool to map");

    if (kafkaConsumerPoolMap.get(className) != null) {
      log.debug("Consumer pool already exists for class::" + className);
      return kafkaConsumerPoolMap.get(className);
    }

    kafkaConsumerPoolMap.put(className, kafkaConsumersPool);

    try {
      kafkaConsumersPool.initialiseKafkaConsumers(kafkaListener, propsConsumer);
    } catch (Exception ex) {
      log.error(
          "Error while initialising Kafka Consumer:: {}, Exception:: {}", ex.getMessage(), ex);
      shutDownKafkaConsumersFactoryPoolGracefully();
    }

    return kafkaConsumersPool;
  }

  public KafkaConsumersPool addConsumerToExistingPool(
      String className, KafkaMessageListener kafkaListener, Properties propsConsumer) {
    KafkaConsumersPool kafkaConsumersPool = new KafkaConsumersPool();

    log.debug("Adding Kafka Consumer Pool to map");

    if (kafkaConsumerPoolMap.get(className) != null) {
      log.debug("overriding the existing consumer pool of {}::", className);
    }

    kafkaConsumerPoolMap.put(className, kafkaConsumersPool);

    try {
      kafkaConsumersPool.initialiseKafkaConsumers(kafkaListener, propsConsumer);
    } catch (Exception ex) {
      log.error(
          "[critical_log_error] Error while initialising Kafka Consumer:: {}, Exception",
          ex.getMessage(),
          ex);
      shutDownKafkaConsumersFactoryPoolGracefully();
    }

    return kafkaConsumersPool;
  }

  /**
   * Create Kafka Consumer Pool & Initializes consumer with default configuration
   *
   * @param className - Class for which consumer pool is generated
   * @param kafkaListener - Listener class to handle consumed messages
   * @return Created Kafka Consumers Pool
   */
  @SuppressWarnings("unused")
  public KafkaConsumersPool createKafkaConsumerPool(
      String className, KafkaMessageListener kafkaListener) {
    KafkaConsumersPool kafkaConsumersPool = new KafkaConsumersPool();

    log.debug("Adding Kafka Consumer Pool to map");

    if (kafkaConsumerPoolMap.get(className) != null) {
      log.debug("Consumer pool already exists for class::" + className);
      return kafkaConsumerPoolMap.get(className);
    }

    kafkaConsumerPoolMap.put(className, kafkaConsumersPool);

    //    try {
    //      kafkaConsumersPool.initialiseKafkaConsumers(kafkaListener, kafkaConfig);
    //    } catch (Exception ex) {
    //      log.error(
    //          "Error while initialising Kafka Consumer:: {}, Exception:: {}", ex.getMessage(),
    // ex);
    //      shutDownKafkaConsumersFactoryPoolGracefully();
    //    }

    return kafkaConsumersPool;
  }

  @SuppressWarnings("unused")
  public void addKafkaConsumerPoolToMap(
      String className, KafkaConsumersPool kafkaConsumersFactory) {
    log.debug("Adding Kafka Consumer Pool to map");
    kafkaConsumerPoolMap.put(className, kafkaConsumersFactory);
  }

  @SuppressWarnings("unused")
  public KafkaConsumersPool getKafkaConsumerPool(String className) {
    return kafkaConsumerPoolMap.get(className) != null ? kafkaConsumerPoolMap.get(className) : null;
  }

  @PreDestroy
  public void shutDownKafkaConsumersFactoryPoolGracefully() {
    log.debug("Shutting down consumer pool gracefully");

    for (KafkaConsumersPool kafkaConsumersPool : kafkaConsumerPoolMap.values()) {
      kafkaConsumersPool.shutDownGracefully();
    }

    kafkaConsumerPoolMap.clear();
  }
}
