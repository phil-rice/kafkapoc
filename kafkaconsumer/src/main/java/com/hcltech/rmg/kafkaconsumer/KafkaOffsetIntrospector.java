// com.example.kafkaconsumer.KafkaOffsetIntrospector
package com.hcltech.rmg.kafkaconsumer;

import com.hcltech.rmg.consumer.abstraction.OffsetIntrospector;
import org.apache.kafka.common.TopicPartition;

public interface KafkaOffsetIntrospector extends OffsetIntrospector<TopicPartition, Long> {}
