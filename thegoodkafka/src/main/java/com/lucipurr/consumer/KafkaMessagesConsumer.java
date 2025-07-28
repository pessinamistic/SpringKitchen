/* (C) Lucipurr 69@420 */
package com.lucipurr.consumer;

import com.lucipurr.util.KafkaConsumerCommitOption;
import com.lucipurr.util.KafkaMessageConsumptionCriteria;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaMessagesConsumer implements Runnable {

  private static final Logger log = LoggerFactory.getLogger(KafkaMessagesConsumer.class);
  private KafkaMessageListener handler;
  private KafkaAllMessagesListener allMessagesHandler;
  String topic;
  Properties props;
  private final Long pollTime;
  private KafkaConsumer<Object, Object> consumer;
  private final AtomicBoolean exit = new AtomicBoolean(false);
  private final AtomicBoolean prepareExit = new AtomicBoolean(false);
  private KafkaConsumerRebalanceListener consumerRebalanceListener;

  public KafkaMessagesConsumer(Properties propsValue, KafkaMessageListener messageHandler) {
    props = propsValue;
    topic = (String) propsValue.get("consumer.topic");
    handler = messageHandler;
    pollTime = (Long) propsValue.get("consumer.poll.time");
  }

  public KafkaMessagesConsumer(
      Properties propsValue,
      KafkaMessageListener messageHandler,
      KafkaConsumerRebalanceListener rebalanceListener) {
    props = propsValue;
    topic = (String) propsValue.get("consumer.topic");
    handler = messageHandler;
    pollTime = (Long) propsValue.get("consumer.poll.time");
    consumerRebalanceListener = rebalanceListener;
  }

  public KafkaMessagesConsumer(
      Properties propsValue,
      KafkaAllMessagesListener allMessagesListener,
      KafkaConsumerRebalanceListener rebalanceListener) {
    props = propsValue;
    topic = (String) propsValue.get("consumer.topic");
    allMessagesHandler = allMessagesListener;
    pollTime = (Long) propsValue.get("consumer.poll.time");
    consumerRebalanceListener = rebalanceListener;
  }

  public AtomicBoolean getPrepareExit() {
    return prepareExit;
  }

  public void setPrepareExit(Boolean prepareExit) {
    this.prepareExit.set(prepareExit);
  }

  @Override
  public void run() {
    try {
      consumer = new KafkaConsumer<>(props);

      Boolean consumerTopicSubscription =
          Boolean.parseBoolean((String) props.get("consumer.topic.subscribe.regex"));

      if (consumerTopicSubscription == null || !consumerTopicSubscription) {
        log.info(
            "Invalid consumer.topic.subscribe.regex: {} received in consumer properties",
            consumerTopicSubscription);
        subscribeConsumer();
      }

      if (consumerTopicSubscription != null && consumerTopicSubscription) {
        subscribeConsumerUsingRegex();
      }

      processMessage(consumer);
    } catch (Exception e) {
      log.error("Message: {} Exception: ", e.getMessage(), e);
      shutDownGracefully();
    }
  }

  private void processMessage(KafkaConsumer<Object, Object> consumer) {

    try (consumer) {
      while (!getPrepareExit().get()) {
        try {

          exit.set(false);
          Duration duration = Duration.ofMillis(pollTime);
          ConsumerRecords<Object, Object> records = consumer.poll(duration);

          if (records.count() > 0) {
            log.debug("Batch size is {}", records.count());
            Integer consumerCommitOption = (Integer) props.get("consumer.commit.option");
            Integer consumerMessageConsumptionCriteria =
                (Integer) props.get("consumer.message.consumption.criteria");

            if (consumerMessageConsumptionCriteria != null
                && consumerMessageConsumptionCriteria.equals(
                    KafkaMessageConsumptionCriteria.PROCESS_ALL_POLLED_RECORDS
                        .getConsumerMessageConsumptionCriteria())) {
              performBulkKafkaUpdates(consumer, consumerCommitOption, records);
            } else {
              performSequentialKafkaUpdates(consumer, consumerCommitOption, records);
            }

            exit.set(true);

          } else {
            exit.set(true);
          }

        } catch (WakeupException we) {
          log.info("wake up exception. break the loop and stop polling kafka.");
          break;
        } catch (Exception excep) {
          log.error("Some exception while reading from kafka:{}", excep.getMessage(), excep);
          boolean isConsumerExitRequired = false;

          if (handler != null) {
            log.info(
                "Checking if consumer close is required for current consumer state using KafkaMessageListener");
            isConsumerExitRequired =
                handler.isConsumerCloseRequiredForCurrentConsumerState(consumer, excep);
          } else if (allMessagesHandler != null) {
            log.info(
                "Checking if consumer close is required for current consumer state using KafkaAllMessageListener");
            isConsumerExitRequired =
                allMessagesHandler.isConsumerCloseRequiredForCurrentConsumerState(consumer, excep);
          }

          if (isConsumerExitRequired) {
            setPrepareExit(true);
          }
        }
      }

    } catch (WakeupException we) {
      log.info("wake up exception. Stop polling kafka.");
    } catch (Exception e) {

      log.error("Exception in startConsuming method : ", e);
    }
  }

  private void performSequentialKafkaUpdates(
      KafkaConsumer<Object, Object> consumer,
      Integer consumerCommitOption,
      ConsumerRecords<Object, Object> records)
      throws Exception {
    if (consumerCommitOption == null) {
      String message =
          String.format(
              "Invalid consumer.commit.option: [%s] received in consumer properties",
              consumerCommitOption);
      log.error(message);
      throw new Exception(message);
    }

    performCommitProcessingChecks(
        consumerCommitOption,
        KafkaConsumerCommitOption.SYNC_COMMIT_BEFORE_PROCESSING_MESSAGES,
        consumer,
        KafkaConsumerCommitOption.ASYNC_COMMIT_BEFORE_PROCESSING_MESSAGES);

    for (ConsumerRecord<Object, Object> oneMsg : records) {
      log.debug("Offset processing {}", oneMsg.offset());
      try {
        handler.onMessage(oneMsg);
      } catch (Exception e) {
        log.error("Exception {} while processing this record {} ", e.getMessage(), oneMsg);
      }
      log.debug("Offset processed {}", oneMsg.offset());
    }

    performCommitProcessingChecks(
        consumerCommitOption,
        KafkaConsumerCommitOption.SYNC_COMMIT_AFTER_PROCESSING_MESSAGES,
        consumer,
        KafkaConsumerCommitOption.ASYNC_COMMIT_AFTER_PROCESSING_MESSAGES);
  }

  private void performBulkKafkaUpdates(
      KafkaConsumer<Object, Object> consumer,
      Integer consumerCommitOption,
      ConsumerRecords<Object, Object> records) {
    if (consumerCommitOption != null) {
      performCommitProcessingChecks(
          consumerCommitOption,
          KafkaConsumerCommitOption.SYNC_COMMIT_BEFORE_PROCESSING_MESSAGES,
          consumer,
          KafkaConsumerCommitOption.ASYNC_COMMIT_BEFORE_PROCESSING_MESSAGES);
    }

    try {
      allMessagesHandler.onMessage(records);
    } catch (Exception e) {
      log.error("Exception {} while processing. Exception: ", e.getMessage(), e);
    }

    if (consumerCommitOption != null) {
      performCommitProcessingChecks(
          consumerCommitOption,
          KafkaConsumerCommitOption.SYNC_COMMIT_AFTER_PROCESSING_MESSAGES,
          consumer,
          KafkaConsumerCommitOption.ASYNC_COMMIT_AFTER_PROCESSING_MESSAGES);
    }
  }

  private static void performCommitProcessingChecks(
      Integer consumerCommitOption,
      KafkaConsumerCommitOption syncCommitProcessingOption,
      KafkaConsumer<Object, Object> consumer,
      KafkaConsumerCommitOption asyncCommitProcessingOption) {
    if (consumerCommitOption.equals(syncCommitProcessingOption.getConsumerCommitOption())) {
      consumer.commitSync();
    }

    if (consumerCommitOption.equals(asyncCommitProcessingOption.getConsumerCommitOption())) {
      consumer.commitAsync();
    }
  }

  private void subscribeConsumerUsingRegex() {

    log.debug("Subscribing to topics matching regex pattern: {}", topic);

    Pattern consumerPattern = Pattern.compile(topic);

    if (consumerRebalanceListener == null) {
      consumer.subscribe(consumerPattern);
      return;
    }

    consumerRebalanceListener.setConsumer(consumer);
    consumer.subscribe(consumerPattern, consumerRebalanceListener);
  }

  private void subscribeConsumer() {

    log.debug("Using default: Subscribing to topic with topic/list of topics: {}", topic);

    List<String> topicsList = new ArrayList<>();

    if (topic.contains(",")) {
      topicsList = Arrays.asList(topic.split(","));
    } else {
      topicsList.add(topic);
    }

    if (consumerRebalanceListener == null) {
      consumer.subscribe(topicsList);
      return;
    }

    consumerRebalanceListener.setConsumer(consumer);
    consumer.subscribe(topicsList, consumerRebalanceListener);
  }

  public void shutDownGracefully() {
    log.info("Starting exit...");

    try {
      if (consumer != null) {

        while (!exit.get()) {
          Thread.sleep(1000);
        }
      }
    } catch (Exception ex) {
      log.error("Exception while waking consumer: {} : ", consumer, ex);
    }

    log.info("Process can be safely terminated...");
  }
}
