/* (C) Lucipurr 69@420 */
package com.lucipurr.beans;

public record KafkaMessageRecord(
    String topic, int partition, long offset, Object key, Object value) {}
