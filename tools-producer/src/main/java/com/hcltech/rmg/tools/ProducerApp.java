package com.hcltech.rmg.tools;

import com.hcltech.rmg.common.TestDomainMessage;
import com.hcltech.rmg.common.codec.Codec;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class ProducerApp {
    private static final Logger log = LoggerFactory.getLogger(ProducerApp.class);

    // -------------------- Admin: ensure topic --------------------
    static void ensureTopic(String bootstrap, String topic, int partitions, short replicationFactor) throws Exception {
        Properties ap = new Properties();
        ap.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);

        try (AdminClient admin = AdminClient.create(ap)) {
            Set<String> existing = admin.listTopics().names().get();

            if (!existing.contains(topic)) {
                log.info("Creating topic '{}' with partitions={} RF={}", topic, partitions, replicationFactor);
                admin.createTopics(List.of(new NewTopic(topic, partitions, replicationFactor))).all().get();
            } else {
                Map<String, TopicDescription> desc = admin.describeTopics(List.of(topic)).all().get();
                int current = desc.get(topic).partitions().size();
                if (current < partitions) {
                    log.info("Increasing partitions for '{}' from {} -> {}", topic, current, partitions);
                    admin.createPartitions(Map.of(topic, NewPartitions.increaseTo(partitions))).all().get();
                } else {
                    log.info("Topic '{}' already has {} partitions (>= requested {}).", topic, current, partitions);
                }
            }
            // Optional: tiny wait for metadata propagation locally
            Thread.sleep(200);
        }
    }

    // -------------------- Producer builder --------------------
    public static KafkaProducer<String, String> newKafkaProducer(Properties app) {
        Properties p = new Properties();

        p.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, get("kafka.bootstrap.servers", app, "localhost:9092"));
        p.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        p.setProperty(ProducerConfig.CLIENT_ID_CONFIG, get("kafka.client.id", app, "producer-app"));

        // Idempotence + ordering-safe pipelining
        p.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, get("kafka.enable.idempotence", app, "true"));
        p.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
                get("kafka.max.in.flight.requests.per.connection", app, "5")); // max allowed with idempotence

        // Batching & compression
        p.setProperty(ProducerConfig.LINGER_MS_CONFIG, get("kafka.linger.ms", app, "10"));
        p.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, get("kafka.batch.size", app, "65536"));
        String compression = get("kafka.compression.type", app, null);
        if (compression != null) p.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, compression);

        // Optional timeouts (wire only if present)
        String delivery = get("kafka.delivery.timeout.ms", app, null);
        if (delivery != null) p.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, delivery);

        String reqTimeout = get("kafka.request.timeout.ms", app, null);
        if (reqTimeout != null) p.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, reqTimeout);

        String maxBlock = get("kafka.max.block.ms", app, null);
        if (maxBlock != null) p.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, maxBlock);

        return new KafkaProducer<>(p);
    }

    // -------------------- Main --------------------
    public static void main(String[] args) throws Exception {
        Properties applicationProperties = loadApplicationProperties();

        // Inputs
        String bootstrap   = get("kafka.bootstrap.servers", applicationProperties, "localhost:9092");
        String topic       = get("kafka.topic", applicationProperties, "test-topic");
        int partitions     = Integer.parseInt(get("kafka.partitions", applicationProperties, "12"));
        short rf           = Short.parseShort(get("kafka.replication.factor", applicationProperties, "1"));

        int count          = Integer.parseInt(get("generator.count", applicationProperties, "100000"));
        int domainCount    = Integer.parseInt(get("generator.domain.count", applicationProperties, "1000"));
        int recordEvery    = Integer.parseInt(get("logging.record.every", applicationProperties, "1000"));
        int parallelism    = Integer.parseInt(get("generator.parallelism", applicationProperties, "100"));
        long seed          = Long.parseLong(get("generator.seed", applicationProperties, "12345"));

        // Ensure topic exists / sized
        ensureTopic(bootstrap, topic, partitions, rf);

        DomainMessageGenerator generator = new DomainMessageGenerator(seed);
        Codec<TestDomainMessage, String> codec = Codec.clazzCodec(TestDomainMessage.class);

        long startMs = System.currentTimeMillis();

        AtomicReference<Throwable> firstError = new AtomicReference<>();
        AtomicLong sent = new AtomicLong();
        Semaphore gate = new Semaphore(parallelism);

        try (KafkaProducer<String, String> producer = newKafkaProducer(applicationProperties)) {
            var it = generator.randomUniformStream(domainCount).limit(count).iterator();

            while (it.hasNext()) {
                if (firstError.get() != null) break; // stop feeding on first failure

                var message = it.next();
                long n = message.count();
                String key = String.valueOf(message.domainId());
                String value = codec.encode(message);

                gate.acquireUninterruptibly(); // cap in-flight sends

                producer.send(new ProducerRecord<>(topic, key, value), (RecordMetadata meta, Exception ex) -> {
                    try {
                        if (ex != null) {
                            if (firstError.compareAndSet(null, ex)) {
                                log.error("First send error: {}", ex.toString());
                                // Optional "early abort":
                                // try { producer.close(Duration.ZERO); } catch (Exception ignore) {}
                            }
                            return;
                        }
                        if (n % recordEvery == 0) {
                            log.info("Produced to {}-{}@{} key={} value={}",
                                    meta.topic(), meta.partition(), meta.offset(), key, value);
                        }
                        sent.incrementAndGet();
                    } finally {
                        gate.release();
                    }
                });
            }

            // Drain outstanding sends
            gate.acquireUninterruptibly(parallelism);
            gate.release(parallelism);

            producer.flush();

            Throwable t = firstError.get();
            if (t != null) throw new RuntimeException("Send failed", t);
        }

        double durationS = (System.currentTimeMillis() - startMs) / 1000.0;
        long sentCount = sent.get();
        log.info("Done. Sent {} messages to topic {}. Took {}s (~{} msg/s)",
                sentCount, topic, durationS, durationS > 0 ? (long)(sentCount / durationS) : sentCount);
    }

    // -------------------- Helpers --------------------
    private static Properties loadApplicationProperties() {
        Properties defaults = new Properties();
        try (InputStream is = ProducerApp.class.getClassLoader().getResourceAsStream("application.properties")) {
            if (is != null) defaults.load(is);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load application.properties", e);
        }
        return defaults;
    }

    private static String get(String key, Properties defaults, String def) {
        String env = System.getenv(toEnvKey(key));
        if (env != null && !env.isBlank()) return env;
        String sys = System.getProperty(key);
        if (sys != null && !sys.isBlank()) return sys;
        return def != null ? defaults.getProperty(key, def) : defaults.getProperty(key);
    }

    private static String toEnvKey(String key) {
        return key.toUpperCase().replace('.', '_').replace('-', '_');
    }
}
