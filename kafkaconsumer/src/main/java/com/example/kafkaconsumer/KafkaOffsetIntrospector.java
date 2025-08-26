// com.example.kafkaconsumer.KafkaOffsetIntrospector
package com.example.kafkaconsumer;

import com.example.kafka.consumer.abstraction.OffsetIntrospector;
import org.apache.kafka.common.TopicPartition;

public interface KafkaOffsetIntrospector extends OffsetIntrospector<TopicPartition, Long> {}
