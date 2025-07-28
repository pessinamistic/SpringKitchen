/* (C) Lucipurr 69@420 */
package com.lucipurr.beans;

import lombok.Data;

@Data
public class KafkaMessage {

  private String topic;
  private int partition;
  private long offset;
  private Object key;
  private Object value;
}
