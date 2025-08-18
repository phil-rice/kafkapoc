package com.example.kafka.consumer;

public class Main {
    public static void main(String[] args) {
        AppConfig config = AppConfig.load();
        ConsumerRunner runner = new ConsumerRunner(config, new SimpleRecordProcessor());
        runner.run();
    }
}
