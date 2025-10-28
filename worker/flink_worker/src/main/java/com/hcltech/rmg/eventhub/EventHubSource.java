package com.hcltech.rmg.eventhub;

import com.azure.messaging.eventhubs.*;
import com.azure.messaging.eventhubs.models.EventPosition;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

public final class EventHubSource implements SourceFunction<Map<String,Object>> {

    private final String connectionString;
    private final String eventHubName;
    private final String consumerGroup;
    private final Duration pollInterval;
    private volatile boolean running = true;

    public EventHubSource(String connectionString,
                          String eventHubName,
                          String consumerGroup,
                          Duration pollInterval) {
        this.connectionString = connectionString;
        this.eventHubName = eventHubName;
        this.consumerGroup = consumerGroup;
        this.pollInterval = (pollInterval == null) ? Duration.ofSeconds(1) : pollInterval;
    }

    @Override
    public void run(SourceContext<Map<String,Object>> ctx) throws Exception {
        EventHubClientBuilder builder = new EventHubClientBuilder()
                .connectionString(connectionString, eventHubName)
                .consumerGroup(consumerGroup);

        try (EventHubConsumerClient client = builder.buildConsumerClient()) {

            for (String partitionId : client.getPartitionIds()) {
                final String partId = partitionId;
                Thread t = new Thread(() -> {
                    EventPosition cursor = EventPosition.latest();
                    while (running) {
                        IterablePartitionEventBatchOptions opts =
                                new IterablePartitionEventBatchOptions()
                                        .setMaximumWaitTime(pollInterval);
                        for (PartitionEvent evt :
                                client.iteratePartitionEvents(partId, cursor, opts)) {
                            if (!running) break;
                            String payload = new String(evt.getData().getBody(), StandardCharsets.UTF_8);
                            synchronized (ctx.getCheckpointLock()) {
                                Map<String,Object> out = new HashMap<>();
                                out.put("rawPayload", payload);
                                out.put("partition", partId);
                                out.put("sequenceNumber", evt.getData().getSequenceNumber());
                                out.put("enqueuedTime", evt.getData().getEnqueuedTime());
                                ctx.collect(out);
                            }
                            cursor = EventPosition.fromSequenceNumber(
                                    evt.getData().getSequenceNumber(), false);
                        }
                        try {
                            Thread.sleep(pollInterval.toMillis());
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }
                }, "eh-partition-"+partId);
                t.setDaemon(true);
                t.start();
            }

            while (running) {
                Thread.sleep(500L);
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
