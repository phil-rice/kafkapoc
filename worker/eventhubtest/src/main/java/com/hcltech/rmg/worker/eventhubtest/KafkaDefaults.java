package com.hcltech.rmg.worker.eventhubtest;

/** Hard defaults you set in code so “normal” runs without env vars. */
public record KafkaDefaults(
        String bootstrap,
        String inputTopic,
        String outputTopic,
        boolean stableGroup // true=reuse group; false=new group each run
) {}
