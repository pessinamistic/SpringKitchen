/* (C) Lucipurr 69@420 */
package com.lucipurr.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class KafkaConsumerGroupUtility {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerGroupUtility.class);
  private static final String CONSUMER_GROUP_APPENDER = "cg_";

  //  @Autowired private Configuration config;
  //  @Autowired private ConfigAccessor configAccessor;

  //  public String fetchConsumerGroup(String key) {
  //    String consumerGroups = getAvailableConsumerGroups(key);
  //    String consumerGroup = getAConsumerGroupForUse(consumerGroups);
  //
  //    if ((consumerGroup == null) || consumerGroup.equals("")) {
  //      LOG.error("Unable to fetch consumer group [{}] from pool of available consumer groups",
  // key);
  //    }
  //
  //    return consumerGroup;
  //  }

  //  private String getAConsumerGroupForUse(String consumerGroups) {
  //    String[] consumerGroupArray = consumerGroups.split(",");
  //    LOG.debug("Available kafka consumer groups array size [{}]", consumerGroupArray.length);
  //    String kafkaCG = "";
  //    String sharedLocksPath = configAccessor.getSharedLocksRoot();
  //    sharedLocksPath += "/kafka";
  //
  //    for (String consumerGroup : consumerGroupArray) {
  //
  //      try {
  //        LOG.debug("Checking if nodepath [{}] already exists in ZK", sharedLocksPath);
  //        if (configAccessor.exists(sharedLocksPath)) {
  //          LOG.debug("Nodepath already exists in ZK [{}]", sharedLocksPath);
  //          List<String> props = configAccessor.getChildren(sharedLocksPath);
  //          boolean isConsumerGroupAvailable = true;
  //          for (String propName : props) {
  //            String[] array = propName.split(CONSUMER_GROUP_APPENDER);
  //
  //            if (array.length <= 1) {
  //              LOG.debug("Property name [{}]", propName);
  //              continue;
  //            }
  //
  //            if (array[1] != null && array[1].equals(consumerGroup)) {
  //              LOG.debug("Consumer Group [{}]is already in use", consumerGroup);
  //              isConsumerGroupAvailable = false;
  //              break;
  //            }
  //          }
  //
  //          if (isConsumerGroupAvailable) {
  //            LOG.debug("Consumer Group [{}] is available", consumerGroup);
  //            kafkaCG = createEphemeralNode(sharedLocksPath, consumerGroup);
  //            break;
  //          } else {
  //            continue;
  //          }
  //        } else {
  //          LOG.debug("Nodepath doesn't exist in ZK: [{}]", sharedLocksPath);
  //          kafkaCG = createEphemeralNode(sharedLocksPath, consumerGroup);
  //          break;
  //        }
  //
  //      } catch (Exception ex) {
  //        LOG.error(
  //            "Error while checking whether node exists :: {}, {}",
  //            ex.getMessage(),
  //            ex.getStackTrace());
  //      }
  //    }
  //
  //    return kafkaCG;
  //  }
  //
  //  private String createEphemeralNode(String nodePath, String consumerGroup) {
  //    nodePath = nodePath + '/' + CONSUMER_GROUP_APPENDER + consumerGroup;
  //    String kafkaCG = "";
  //
  //    try {
  //      LOG.debug(
  //          "Creating ephemeral node for consumer group [{}] at path [{}]", consumerGroup,
  // nodePath);
  //      configAccessor.createEphemeralNodeString(nodePath, consumerGroup);
  //      LOG.debug(
  //          "Created ephemeral node for consumer group [{}] at path [{}]", consumerGroup,
  // nodePath);
  //
  //      kafkaCG = consumerGroup;
  //    } catch (Exception ex) {
  //      LOG.error("Error while creating ephemeral node [{}]", ex.getMessage(), ex);
  //    }
  //
  //    return kafkaCG;
  //  }
  //
  //  private String getAvailableConsumerGroups(String key) {
  //
  //    String availableConsumerGroups = config.get(key);
  //    LOG.debug("Available Kafka Consumer Groups [{}]", availableConsumerGroups);
  //
  //    return availableConsumerGroups;
  //  }
}
