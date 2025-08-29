/* (C) Lucipurr 69@420 */
package com.lucipurr.beans;

import java.util.List;

public record KafkaBatchMessages(long messageCount, List<KafkaMessageRecord> messages) {}
