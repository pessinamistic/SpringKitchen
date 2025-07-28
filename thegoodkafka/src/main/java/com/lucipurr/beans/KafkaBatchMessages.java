/* (C) Lucipurr 69@420 */
package com.lucipurr.beans;

import java.util.List;
import lombok.Data;

@Data
public class KafkaBatchMessages {
  private long messageCount;
  private List<KafkaMessage> messages;
}
