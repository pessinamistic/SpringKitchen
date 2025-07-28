/* (C) Lucipurr 69@420 */
package com.lucipurr.util;

public enum KafkaConsumerCommitOption {
  SYNC_COMMIT_BEFORE_PROCESSING_MESSAGES(1),
  SYNC_COMMIT_AFTER_PROCESSING_MESSAGES(2),
  ASYNC_COMMIT_BEFORE_PROCESSING_MESSAGES(3),
  ASYNC_COMMIT_AFTER_PROCESSING_MESSAGES(4);

  private Integer consumerCommitOption;

  KafkaConsumerCommitOption(Integer consumerCommitOption) {
    this.consumerCommitOption = consumerCommitOption;
  }

  public Integer getConsumerCommitOption() {
    return this.consumerCommitOption;
  }
}
