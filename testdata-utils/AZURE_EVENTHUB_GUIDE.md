# Azure Event Hub Integration Guide

## Overview

The `lmdb_kafka_streamer.py` now supports **Azure Event Hubs** in addition to Apache Kafka. Event Hubs provides a Kafka-compatible endpoint, so you can use the same tool to send data to either platform.

---

## Key Features

✅ **Kafka-compatible**: Uses Event Hub's Kafka endpoint
✅ **Same commands**: Just add `--producer-type eventhub`
✅ **SASL/SSL security**: Automatic secure connection
✅ **Same performance**: High throughput with Event Hubs
✅ **Same data format**: Headers, keys, XML body unchanged

---

## Prerequisites

### 1. Azure Event Hub Setup

You need:

- An **Azure Event Hubs namespace** (e.g., `mynamespace`)
- An **Event Hub** (topic equivalent, e.g., `mper-input-events`)
- A **connection string** with send permissions

### 2. Get Your Connection String

From Azure Portal:

1. Go to your Event Hubs namespace
2. Click **"Shared access policies"**
3. Select a policy with **Send** permissions (or create one)
4. Copy the **"Connection string–primary key"**

Example connection string format:

```
Endpoint=sb://mynamespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=YOUR_KEY_HERE;EntityPath=mper-input-events
```

---

## Usage Examples

### Example 1: Send to Kafka (Default)

```bash
python lmdb_kafka_streamer.py emit-chronological \
  --db ./event_buffer_lmdb \
  --bootstrap-servers localhost:9092 \
  --topic mper-input-events \
  --partitions 1
```

### Example 2: Send to Azure Event Hub

```bash
python lmdb_kafka_streamer.py emit-chronological \
  --db ./event_buffer_lmdb \
  --bootstrap-servers mynamespace.servicebus.windows.net:9093 \
  --topic mper-input-events \
  --partitions 1 \
  --producer-type eventhub \
  --eventhub-connection-string "Endpoint=sb://mynamespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=YOUR_KEY_HERE;EntityPath=mper-input-events"
```

**Note:** Put the connection string in quotes to preserve special characters.

### Example 3: Using Environment Variable

For security, store the connection string in an environment variable:

```bash
# Set environment variable
export EVENTHUB_CONN_STR="Endpoint=sb://mynamespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=YOUR_KEY_HERE;EntityPath=mper-input-events"

# Use in command
python lmdb_kafka_streamer.py emit-chronological \
  --db ./event_buffer_lmdb \
  --bootstrap-servers mynamespace.servicebus.windows.net:9093 \
  --topic mper-input-events \
  --partitions 1 \
  --producer-type eventhub \
  --eventhub-connection-string "$EVENTHUB_CONN_STR"
```

### Example 4: Emit Specific Scan to Event Hub

```bash
python lmdb_kafka_streamer.py emit-scan \
  --scan 1 \
  --db ./event_buffer_lmdb \
  --bootstrap-servers mynamespace.servicebus.windows.net:9093 \
  --topic mper-input-events \
  --partitions 1 \
  --producer-type eventhub \
  --eventhub-connection-string "$EVENTHUB_CONN_STR"
```

### Example 5: FastAPI Server with Event Hub

```bash
python lmdb_kafka_streamer.py serve \
  --db ./event_buffer_lmdb \
  --bootstrap-servers mynamespace.servicebus.windows.net:9093 \
  --topic mper-input-events \
  --partitions 1 \
  --host 0.0.0.0 \
  --port 8080 \
  --producer-type eventhub \
  --eventhub-connection-string "$EVENTHUB_CONN_STR"
```

---

## Command-Line Arguments

### Required for Event Hub

| Argument                       | Description                      | Example                                                       |
| ------------------------------ | -------------------------------- | ------------------------------------------------------------- |
| `--producer-type`              | Set to `eventhub`                | `--producer-type eventhub`                                    |
| `--eventhub-connection-string` | Your Event Hub connection string | `--eventhub-connection-string "Endpoint=..."`                 |
| `--bootstrap-servers`          | Event Hub namespace endpoint     | `--bootstrap-servers mynamespace.servicebus.windows.net:9093` |
| `--topic`                      | Event Hub name                   | `--topic mper-input-events`                                   |

### Common Settings

| Argument       | Default | Notes                     |
| -------------- | ------- | ------------------------- |
| `--partitions` | 1       | Use 1 for strict ordering |
| `--acks`       | all     | Use "all" for durability  |
| `--limit`      | None    | Limit number of messages  |

---

## Configuration Details

### Event Hub Kafka Endpoint

When you use `--producer-type eventhub`, the tool automatically configures:

```python
{
    "bootstrap.servers": "mynamespace.servicebus.windows.net:9093",
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "PLAIN",
    "sasl.username": "$ConnectionString",
    "sasl.password": "<your-connection-string>",
    "compression.type": "zstd",
    "linger.ms": 200,
    "batch.num.messages": 20000,
    # ... high-throughput settings
}
```

**Port:** Event Hubs uses port **9093** for Kafka protocol (not 9092).

---

## Comparison: Kafka vs Event Hub

| Feature         | Kafka        | Event Hub             | Notes                                |
| --------------- | ------------ | --------------------- | ------------------------------------ |
| **Protocol**    | Native Kafka | Kafka-compatible      | Same tool works for both             |
| **Port**        | 9092         | 9093                  | Different default ports              |
| **Auth**        | Various      | SASL/SSL              | Event Hub requires connection string |
| **Partitions**  | Flexible     | 1-32 (tier dependent) | Use 1 for strict ordering            |
| **Performance** | High         | High                  | Similar throughput                   |
| **Headers**     | ✅           | ✅                    | Same format                          |
| **Keys**        | ✅           | ✅                    | Same format                          |
| **Timestamps**  | ✅           | ✅                    | Same format                          |

---

## Performance Expectations

### With Event Hub

- **Throughput:** 40,000-60,000 msg/s (similar to Kafka)
- **Latency:** Slightly higher due to cloud networking
- **Compression:** zstd works well
- **Batching:** 20k messages per batch

### Example Output

```
[eventhub] Connecting to mynamespace.servicebus.windows.net:9093
[stats] 2025-10-28T15:23:45Z  rate=   42341 msg/s  avg=   41234 msg/s  total=2,534,123  out=15,234
[stats] 2025-10-28T15:23:46Z  rate=   43892 msg/s  avg=   41456 msg/s  total=2,578,015  out=14,892
[summary] Sent 3,000,000 messages in 72.1s (41,608 msg/s)
```

---

## Troubleshooting

### Error: "Event Hub connection string required"

**Problem:** Missing `--eventhub-connection-string` when using `--producer-type eventhub`

**Solution:**

```bash
--eventhub-connection-string "Endpoint=sb://..."
```

### Error: "Connection refused" or "timeout"

**Problem:** Wrong port or endpoint

**Solution:** Ensure you're using:

- Port **9093** (not 9092)
- Format: `namespace.servicebus.windows.net:9093`

Example:

```bash
--bootstrap-servers mynamespace.servicebus.windows.net:9093
```

### Error: "Authentication failed"

**Problem:** Invalid connection string or expired key

**Solution:**

1. Verify connection string is correct (check for typos)
2. Ensure the policy has **Send** permissions
3. Check if the key has been rotated/regenerated

### Error: "Topic does not exist"

**Problem:** Event Hub name doesn't match

**Solution:** Ensure `--topic` matches your Event Hub name exactly:

```bash
--topic mper-input-events  # Must match Event Hub name
```

### Slow Performance

**Problem:** Lower throughput than expected

**Solutions:**

1. Increase Event Hub throughput units (Azure Portal)
2. Use Standard or Premium tier for better performance
3. Check network latency (use Azure VM in same region)
4. Verify `--partitions 1` for your use case

---

## Security Best Practices

### 1. Never Hard-Code Connection Strings

❌ **Bad:**

```bash
--eventhub-connection-string "Endpoint=sb://...;SharedAccessKey=SECRET_KEY"
```

✅ **Good:**

```bash
export EVENTHUB_CONN_STR="..."
--eventhub-connection-string "$EVENTHUB_CONN_STR"
```

### 2. Use Least-Privilege Policies

Create a policy with **only Send** permissions:

```
Policy Name: SendOnlyPolicy
Claims: Send
```

### 3. Rotate Keys Regularly

- Regenerate SharedAccessKeys periodically
- Update environment variables
- Use Azure Key Vault for production

### 4. Use Managed Identity (Production)

For production Azure VMs, use Managed Identity instead of connection strings:

```python
# Future enhancement - Managed Identity support
# No connection string needed
```

---

## Complete Workflow Example

### Step 1: Generate Test Data

```bash
python lmdb_kafka_streamer.py generate \
  --parcels 100000 \
  --db ./event_buffer_lmdb \
  --seed 42
```

### Step 2: Send to Event Hub

```bash
# Set connection string once
export EVENTHUB_CONN_STR="Endpoint=sb://mynamespace.servicebus.windows.net/;SharedAccessKeyName=SendPolicy;SharedAccessKey=YOUR_KEY;EntityPath=mper-input-events"

# Send all events
python lmdb_kafka_streamer.py emit-chronological \
  --db ./event_buffer_lmdb \
  --bootstrap-servers mynamespace.servicebus.windows.net:9093 \
  --topic mper-input-events \
  --partitions 1 \
  --producer-type eventhub \
  --eventhub-connection-string "$EVENTHUB_CONN_STR"
```

### Step 3: Verify in Azure Portal

1. Go to your Event Hub in Azure Portal
2. Click **"Metrics"**
3. Check **"Incoming Messages"** graph
4. You should see 400,000 messages (100k parcels × 4 events)

---

## Monitoring and Metrics

### Azure Portal Metrics

Monitor your Event Hub:

- **Incoming Messages**: Total messages sent
- **Incoming Bytes**: Data volume
- **Throttled Requests**: If you hit limits
- **Server Errors**: Connection/auth issues
- **Successful Requests**: Successful sends

### Application Logs

The tool prints live stats:

```
[eventhub] Connecting to mynamespace.servicebus.windows.net:9093
[stats] ... rate=42341 msg/s ...
[summary] Sent 3,000,000 messages in 72.1s (41,608 msg/s)
```

---

## Event Hub Tiers

| Tier          | Throughput Units | Max Ingress   | Price |
| ------------- | ---------------- | ------------- | ----- |
| **Basic**     | 1-20 TUs         | 1 MB/s per TU | $     |
| **Standard**  | 1-20 TUs         | 1 MB/s per TU | $$    |
| **Premium**   | 1-100 PUs        | Higher        | $$$   |
| **Dedicated** | CUs              | Highest       | $$$$  |

**Recommendation:** Start with Standard tier (1 TU) for testing, scale up as needed.

---

## FAQ

### Q: Can I use the same LMDB database for both Kafka and Event Hub?

**A:** Yes! The database is producer-agnostic. Generate once, emit to either:

```bash
# Generate once
python lmdb_kafka_streamer.py generate --parcels 100000 --db ./db

# Send to Kafka
python lmdb_kafka_streamer.py emit-chronological --db ./db ... --producer-type kafka

# Send to Event Hub
python lmdb_kafka_streamer.py emit-chronological --db ./db ... --producer-type eventhub
```

### Q: Are messages identical between Kafka and Event Hub?

**A:** Yes. Same XML, same headers, same keys, same timestamps.

### Q: Can I emit to both simultaneously?

**A:** Not in one command, but you can run two separate commands:

```bash
# Terminal 1: Send to Kafka
python lmdb_kafka_streamer.py emit-chronological --db ./db ... --producer-type kafka

# Terminal 2: Send to Event Hub
python lmdb_kafka_streamer.py emit-chronological --db ./db ... --producer-type eventhub
```

**Note:** Since deletion happens, generate separate databases or disable deletion.

### Q: What about Event Hub consumer groups?

**A:** Consumer groups are managed on the Event Hub side (Azure Portal). The producer doesn't need to know about them.

### Q: Does this work with Event Hub Capture?

**A:** Yes! If you enable Event Hub Capture (saves to Blob/Data Lake), captured messages will include all headers and keys.

---

## Migration: Kafka → Event Hub

### Checklist

- [ ] Create Event Hubs namespace in Azure
- [ ] Create Event Hub (same name as Kafka topic)
- [ ] Get connection string with Send permissions
- [ ] Update commands: add `--producer-type eventhub`
- [ ] Update commands: add `--eventhub-connection-string`
- [ ] Change port: 9092 → 9093
- [ ] Test with small dataset (1000 parcels)
- [ ] Monitor metrics in Azure Portal
- [ ] Scale up throughput units if needed
- [ ] Update consumers to read from Event Hub

---

## Support and Resources

### Azure Event Hubs Documentation

- [Event Hubs for Kafka](https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-for-kafka-ecosystem-overview)
- [Kafka protocol support](https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-kafka-protocol-guide)
- [Connection strings](https://docs.microsoft.com/en-us/azure/event-hubs/event-hubs-get-connection-string)

### Confluent Kafka Client

- [confluent-kafka-python](https://docs.confluent.io/kafka-clients/python/current/overview.html)
- [SASL/SSL configuration](https://docs.confluent.io/platform/current/kafka/authentication_sasl/index.html)

---

## Summary

✅ **Easy Switch:** Just add `--producer-type eventhub`
✅ **Secure:** SASL/SSL automatic
✅ **Fast:** Same 40k+ msg/s performance
✅ **Compatible:** Same data format
✅ **Flexible:** Use Kafka or Event Hub with same tool

**Ready to send to Azure Event Hubs!** 🚀
