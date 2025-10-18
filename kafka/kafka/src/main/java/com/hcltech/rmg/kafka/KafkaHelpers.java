package com.hcltech.rmg.kafka;

import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.concurrent.TimeUnit;

public class KafkaHelpers {

    public  static <Inp, Out> SingleOutputStreamOperator<Out> liftFunctionToOrderedAsync(
            SingleOutputStreamOperator<Inp> input, String name, RichAsyncFunction<Inp, Out> func, int totalPartitions, int lanes, long asyncTimeoutMillis) {

        int partitionsPerSubtask = (int) Math.ceil((double) totalPartitions / Math.max(1, totalPartitions));
        int capacity = Math.max(1, (int) Math.round(lanes * partitionsPerSubtask * 1.2));

        return AsyncDataStream
                .orderedWait(input, func, asyncTimeoutMillis, TimeUnit.MILLISECONDS, capacity)
                .name(name)
                .setParallelism(totalPartitions);
    }

}
