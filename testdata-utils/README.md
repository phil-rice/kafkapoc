### Generate a small sample (100 parcels)

```bash
python generate_rm_full_dataset_v2.py --parcels 100 --output-dir out_sample --batch-size 200
```

### Generate 10 million parcels

```bash
python generate_rm_full_dataset_v2.py --parcels 10000000 --output-dir out_10M_parcels --batch-size 100000 --seed 42
```

### Generate 10 million total events

For 10 million total input events (2.5M parcels × 4 scans each):

```bash
python generate_rm_full_dataset_v2.py --parcels 2500000 --output-dir out_10M_events --batch-size 100000 --seed 42
```

## Optional Arguments

| Argument          | Description                                 | Default          |
| ----------------- | ------------------------------------------- | ---------------- |
| `--parcels`       | Number of parcels to generate               | 100              |
| `--output-dir`    | Destination directory for output            | `out_full`       |
| `--batch-size`    | Number of XML files per zip                 | 100000           |
| `--postcode-file` | Path to custom postcode list (one per line) | built-in list    |
| `--seed`          | Random seed for reproducibility             | 42               |
| `--start-date`    | Earliest generation date (YYYY-MM-DD)       | today - 365 days |
| `--end-date`      | Latest generation date (YYYY-MM-DD)         | today            |

## Output Files

| File                        | Description                              |
| --------------------------- | ---------------------------------------- |
| `postcode.csv`              | Base postcode and address metadata       |
| `postcode_suffix.csv`       | Per-postcode DPS suffixes                |
| `notifications_summary.csv` | Summary of notification types per parcel |
| `inputs/*.zip`              | Raw scan XMLs (4 per parcel)             |
| `outputs/*.zip`             | Notification XMLs (email/SMS)            |
| `combined/*.zip`            | Combined input-output test XMLs          |


## Working with Generated Data

### Extracting ZIP files

Extract all input XMLs to a flat directory for processing:

```bash
find ./out_10K/inputs -type f -name "*.zip" -size +0c -print0 | xargs -0 -n 1 -P 8 sh -c 'unzip -q -o "$0" -d "'"$(pwd)"'/flat_inputs"'
```
```bash
find ./out_100K/inputs -type f -name "\*.zip" -size +0c -print0 | xargs -0 -n 1 -P 8 sh -c 'unzip -q -o "$0" -d ./flat_inputs'

find ./out_10K/inputs -type f -name "\*.zip" -size +0c -print0 | xargs -0 -n 1 -P 8 sh -c 'unzip -q -o "$0" -d "'"$(pwd)"'/flat_inputs"'

find ./out_10K/inputs -type f -name "\*.zip" -size +0c -print0 | xargs -0 -n 1 -P 8 sh -c 'unzip -q -o "$0" -d "'"$(pwd)"'/flat_inputs"'
```

### Publishing to Kafka (Java)

```bash
java -jar target/producer-java-jar-with-dependencies.jar --brokers=localhost:9092 --topic=mper-input-events --input=../flat_inputs --reader-threads=4 --sender-threads=8
```

### Publishing to Kafka (Python)

```bash
python3 kafka_producer.py --inputs-dir ./flat_inputs/ --bootstrap-servers localhost:9092 --topic mper-input-events --create-topics --partitions 12 --retention-ms 604800000 --linger-ms 500 --compression zstd --max-outstanding 300000 --zip-cache-size 1 --time-compression 0 --min-sleep-ms 0.1
```
### Performance Benchmarks

#### 4 Workers (100,000 messages)

| Time                | Rate (msg/s) | Avg (msg/s) | Total   | Outstanding |
| ------------------- | ------------ | ----------- | ------- | ----------- |
| 2025-10-26 19:49:25 | 19,788       | 19,788      | 19,792  | 518         |
| 2025-10-26 19:49:26 | 18,960       | 19,374      | 38,783  | 407         |
| 2025-10-26 19:49:27 | 19,001       | 19,250      | 57,789  | 335         |
| 2025-10-26 19:49:28 | 17,088       | 18,709      | 74,880  | 741         |
| 2025-10-26 19:49:29 | 19,103       | 18,788      | 94,081  | 623         |
| **Summary**         |              |             | 100,000 | 5.5s (18,135 msg/s) |

#### 8 Workers (100,000 messages)

| Time                | Rate (msg/s) | Avg (msg/s) | Total   | Outstanding |
| ------------------- | ------------ | ----------- | ------- | ----------- |
| 2025-10-26 19:59:52 | 27,569       | 27,569      | 27,711  | 5           |
| 2025-10-26 19:59:53 | 23,017       | 25,293      | 50,843  | 125         |
| 2025-10-26 19:59:54 | 23,854       | 24,814      | 74,809  | 165         |
| 2025-10-26 19:59:55 | 24,439       | 24,720      | 99,294  | 191         |
| **Summary**         |              |             | 100,000 | 4.5s (22,439 msg/s) |