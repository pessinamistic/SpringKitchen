/* (C) Lucipurr 69@420 */
package com.lucipurr.util;

public enum KafkaMessageConsumptionCriteria {
  PROCESS_ALL_POLLED_RECORDS(1),
  PROCESS_ONE_RECORD(2);

  private Integer consumerMessageConsumptionCriteria;

  KafkaMessageConsumptionCriteria(Integer consumerMessageConsumptionCriteria) {
    this.consumerMessageConsumptionCriteria = consumerMessageConsumptionCriteria;
  }

  public Integer getConsumerMessageConsumptionCriteria() {
    return this.consumerMessageConsumptionCriteria;
  }
}
