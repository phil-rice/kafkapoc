/*
ParallelKafkaProducer.java

High-throughput Java Kafka producer that:
- Parses XML scan events (StAX) from input files (supports .zip with entries or plain .xml files)
- Produces messages to Kafka in parallel using a reader -> queue -> sender pipeline
- Includes Kafka message headers (event_code, scan_index, original_ts, parsed_ts)
- Uses configurable producer tuning for max throughput (batch.size, linger.ms, compression.type, acks, buffer.memory)
- Reports throughput periodically

Build: Use Maven with dependency on kafka-clients (see included pom snippet at bottom)

Notes:
- This is a self-contained example focusing on performance and correctness. Tune thread counts, queue size and producer configs to match your environment.
*/

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class ParallelKafkaProducer {

    // Simple data holder for messages to send
    public static class SendMessage {
        public final String key;
        public final String value; // XML payload (or extracted JSON if you prefer)
        public final Map<String, String> headers;

        public SendMessage(String key, String value, Map<String, String> headers) {
            this.key = key;
            this.value = value;
            this.headers = headers;
        }
    }

    // Configuration defaults
    private static final String DEFAULT_TOPIC = "scans";
    private static final int DEFAULT_READER_THREADS = 4;
    private static final int DEFAULT_SENDER_THREADS = 8;
    private static final int DEFAULT_QUEUE_CAPACITY = 100_000;
    private static final int METRICS_PRINT_INTERVAL_SEC = 5;

    public static void main(String[] args) throws Exception {
        // Basic CLI parsing (very small): args can include --brokers, --topic, --input-dir, --reader-threads, --sender-threads
        Map<String,String> cli = parseCli(args);
        String brokers = cli.getOrDefault("brokers","localhost:9092");
        String topic = cli.getOrDefault("topic", DEFAULT_TOPIC);
        String inputPath = cli.getOrDefault("input","./input");
        int readerThreads = Integer.parseInt(cli.getOrDefault("reader-threads", Integer.toString(DEFAULT_READER_THREADS)));
        int senderThreads = Integer.parseInt(cli.getOrDefault("sender-threads", Integer.toString(DEFAULT_SENDER_THREADS)));
        int queueCapacity = Integer.parseInt(cli.getOrDefault("queue-capacity", Integer.toString(DEFAULT_QUEUE_CAPACITY)));

        System.out.println("ParallelKafkaProducer starting");
        System.out.println("brokers="+brokers+" topic="+topic+" input="+inputPath+" readers="+readerThreads+" senders="+senderThreads);

        BlockingQueue<SendMessage> queue = new ArrayBlockingQueue<>(queueCapacity);
        AtomicBoolean finishedReading = new AtomicBoolean(false);
        AtomicLong producedCount = new AtomicLong(0);
        AtomicLong failedCount = new AtomicLong(0);
        AtomicLong inFlight = new AtomicLong(0);

        // Start reader pool
        ExecutorService readerPool = Executors.newFixedThreadPool(readerThreads, r -> new Thread(r, "reader-"+r.hashCode()));

        // Discover files to process (zip or xml)
        List<Path> inputs = discoverInputFiles(inputPath);
        System.out.println("Discovered " + inputs.size() + " input files to read");

        // Distribute files across readers
        final Iterator<Path> it = inputs.iterator();
        for (int i=0;i<readerThreads;i++) {
            readerPool.submit(() -> {
                while (true) {
                    Path p;
                    synchronized (it) {
                        if (!it.hasNext()) break;
                        p = it.next();
                    }
                    try {
                        if (p.toString().endsWith(".zip")) {
                            processZipFile(p, queue);
                        } else {
                            processXmlFile(p, queue);
                        }
                    } catch (Exception e) {
                        System.err.println("Error processing " + p + " : " + e.getMessage());
                        e.printStackTrace(System.err);
                    }
                }
            });
        }

        // Create sender pool - each sender will have its own KafkaProducer instance to avoid contention
        ExecutorService senderPool = Executors.newFixedThreadPool(senderThreads, r -> new Thread(r, "sender-"+r.hashCode()));

        // Build base producer properties tuned for throughput
        Properties baseProps = new Properties();
        baseProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        baseProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        baseProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // Tuning for throughput
        baseProps.put(ProducerConfig.ACKS_CONFIG, "1");                 // lower latency, higher throughput
        baseProps.put(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(256 * 1024)); // 256 KB batch
        baseProps.put(ProducerConfig.LINGER_MS_CONFIG, Integer.toString(20)); // wait up to 20ms for batching
        baseProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");    // lz4 is fast and gives good compression
        baseProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, Long.toString(256L * 1024L * 1024L)); // 256 MB
        baseProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, Integer.toString(5));
        baseProps.put(ProducerConfig.RETRIES_CONFIG, Integer.toString(3));

        // Start sender threads
        for (int i=0;i<senderThreads;i++) {
            senderPool.submit(() -> {
                KafkaProducer<String,String> producer = new KafkaProducer<>(baseProps);
                try {
                    while (true) {
                        SendMessage sm = queue.poll(1, TimeUnit.SECONDS);
                        if (sm == null) {
                            // if readers finished and queue empty -> exit
                            if (finishedReading.get() && queue.isEmpty()) break;
                            continue;
                        }

                        // Build ProducerRecord with headers
                        ProducerRecord<String,String> rec = new ProducerRecord<>(topic, sm.key, sm.value);
                        if (sm.headers != null) {
                            for (Map.Entry<String,String> e : sm.headers.entrySet()) {
                                if (e.getValue() != null) {
                                    Header h = new RecordHeader(e.getKey(), e.getValue().getBytes(StandardCharsets.UTF_8));
                                    rec.headers().add(h);
                                }
                            }
                        }

                        inFlight.incrementAndGet();
                        producer.send(rec, new Callback() {
                            @Override
                            public void onCompletion(RecordMetadata metadata, Exception exception) {
                                inFlight.decrementAndGet();
                                if (exception != null) {
                                    failedCount.incrementAndGet();
                                    // log minimal to avoid overhead
                                } else {
                                    producedCount.incrementAndGet();
                                }
                            }
                        });
                    }

                    // flush outstanding
                    producer.flush();
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                } finally {
                    producer.close();
                }
            });
        }

        // Metrics reporter
        ScheduledExecutorService metrics = Executors.newSingleThreadScheduledExecutor();
        final long[] lastCount = {0};
        metrics.scheduleAtFixedRate(() -> {
            long now = producedCount.get();
            long delta = now - lastCount[0];
            lastCount[0] = now;
            System.out.printf("%s: produced=%d failed=%d inflight=%d rate=%,d msg/s\n",
                    Instant.now().toString(), now, failedCount.get(), inFlight.get(), delta / METRICS_PRINT_INTERVAL_SEC);
        }, METRICS_PRINT_INTERVAL_SEC, METRICS_PRINT_INTERVAL_SEC, TimeUnit.SECONDS);

        // Wait for readers to finish
        readerPool.shutdown();
        readerPool.awaitTermination(1, TimeUnit.DAYS);
        finishedReading.set(true);
        System.out.println("Readers finished; waiting for senders to drain queue...");

        // Wait for senders
        senderPool.shutdown();
        senderPool.awaitTermination(1, TimeUnit.DAYS);

        // Final metrics and shutdown
        metrics.shutdown();
        System.out.println("All senders done.");
        System.out.printf("Final: produced=%d failed=%d\n", producedCount.get(), failedCount.get());
    }

    // Minimal CLI parser: handles --key=value or --flag
    private static Map<String,String> parseCli(String[] args) {
        Map<String,String> m = new HashMap<>();
        for (String a: args) {
            if (a.startsWith("--")) {
                String t = a.substring(2);
                int eq = t.indexOf('=');
                if (eq >= 0) {
                    m.put(t.substring(0,eq), t.substring(eq+1));
                } else {
                    m.put(t, "true");
                }
            }
        }
        return m;
    }

    // Discover files (recursively) under inputPath
    private static List<Path> discoverInputFiles(String inputPath) throws IOException {
        Path p = Paths.get(inputPath);
        List<Path> found = new ArrayList<>();
        if (!Files.exists(p)) return found;
        Files.walk(p)
                .filter(Files::isRegularFile)
                .filter(pp -> pp.toString().endsWith(".zip") || pp.toString().endsWith(".xml"))
                .forEach(found::add);
        return found;
    }

    // Process ZIP file: iterate entries and parse entries with .xml extension
    private static void processZipFile(Path zipPath, BlockingQueue<SendMessage> queue) throws IOException {
        try (InputStream fis = Files.newInputStream(zipPath);
             ZipInputStream zis = new ZipInputStream(fis)) {
            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                if (!entry.isDirectory() && entry.getName().toLowerCase().endsWith(".xml")) {
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    byte[] buf = new byte[8192];
                    int r;
                    while ((r = zis.read(buf)) != -1) baos.write(buf,0,r);
                    byte[] xmlBytes = baos.toByteArray();
                    // parse this XML stream to extract messages (may contain multiple scan events)
                    try (InputStream is2 = new ByteArrayInputStream(xmlBytes)) {
                        feedXmlStream(is2, queue);
                    } catch (Exception e) {
                        System.err.println("XML parse error in " + zipPath + "!" + entry.getName() + " : " + e.getMessage());
                    }
                }
                zis.closeEntry();
            }
        }
    }

    // Process a plain XML file
    private static void processXmlFile(Path xmlPath, BlockingQueue<SendMessage> queue) throws IOException {
        try (InputStream is = Files.newInputStream(xmlPath)) {
            feedXmlStream(is, queue);
        } catch (Exception e) {
            System.err.println("XML parse error in " + xmlPath + " : " + e.getMessage());
        }
    }

    // Parse an input XML stream using StAX (fast), emit messages into the queue
    private static void feedXmlStream(InputStream is, BlockingQueue<SendMessage> queue) throws XMLStreamException, InterruptedException {
        XMLInputFactory factory = XMLInputFactory.newInstance();
        XMLStreamReader reader = factory.createXMLStreamReader(is, "UTF-8");

        // Heuristic: assume XML contains repeated <scan>...</scan> elements
        // We'll extract the full text of each <scan> element as the message value.
        while (reader.hasNext()) {
            int event = reader.next();
            if (event == XMLStreamConstants.START_ELEMENT && reader.getLocalName().equalsIgnoreCase("manualScan")) {
                StringWriter sw = new StringWriter();
                // write open tag with attributes
                sw.write("<scan");
                for (int i=0;i<reader.getAttributeCount();i++) {
                    sw.write(" ");
                    sw.write(reader.getAttributeLocalName(i));
                    sw.write("=\"");
                    sw.write(escapeXml(reader.getAttributeValue(i)));
                    sw.write("\"");
                }
                sw.write(">\n");

                // read inner XML until corresponding END_ELEMENT
                int depth = 1;
                while (reader.hasNext() && depth > 0) {
                    int e = reader.next();
                    switch (e) {
                        case XMLStreamConstants.START_ELEMENT:
                            sw.write("<" + reader.getLocalName());
                            for (int i=0;i<reader.getAttributeCount();i++) {
                                sw.write(" ");
                                sw.write(reader.getAttributeLocalName(i));
                                sw.write("=\"");
                                sw.write(escapeXml(reader.getAttributeValue(i)));
                                sw.write("\"");
                            }
                            sw.write(">");
                            depth++;
                            break;
                        case XMLStreamConstants.CHARACTERS:
                            sw.write(escapeXml(reader.getText()));
                            break;
                        case XMLStreamConstants.END_ELEMENT:
                            sw.write("</"+reader.getLocalName()+">");
                            depth--;
                            break;
                        case XMLStreamConstants.CDATA:
                            sw.write(reader.getText());
                            break;
                        default:
                            break;
                    }
                }

                String scanXml = sw.toString();

                // Optionally extract a key (e.g., parcel id) and headers from the scanXml
                String key = extractTagValue(scanXml, "parcelId");
                if (key == null) key = UUID.randomUUID().toString();
                Map<String,String> headers = new HashMap<>();

                String eventCode = extractTagValue(scanXml, "eventCode");
                if (eventCode != null) headers.put("event_code", eventCode);

                String scanIndex = extractTagValue(scanXml, "scanIndex");
                if (scanIndex != null) headers.put("scan_index", scanIndex);

                String domainId = extractTagValue(scanXml, "uniqueItemId");
                if (domainId != null) headers.put("domainId", domainId);

                String originalTs = extractTagValue(scanXml, "timestamp");
                if (originalTs != null) headers.put("original_ts", originalTs);

                headers.put("parsed_ts", Instant.now().toString());


                SendMessage sm = new SendMessage(key, scanXml, headers);

                // Blocking put (backpressure) to avoid unlimited memory usage
                queue.put(sm);
            }
        }

        reader.close();
    }

    // Very small XML escape for attribute/text safety
    private static String escapeXml(String s) {
        if (s == null) return "";
        return s.replace("&","&amp;")
                .replace("<","&lt;")
                .replace(">","&gt;")
                .replace("\"","&quot;")
                .replace("'","&apos;");
    }

    // Very naive tag value extractor: finds <tag>value</tag> in xml string. For speed it's string-based; for strictness use an XML parser.
    private static String extractTagValue(String xml, String tag) {
        String open = "<"+tag+">";
        String close = "</"+tag+">";
        int a = xml.indexOf(open);
        if (a < 0) return null;
        int b = xml.indexOf(close, a+open.length());
        if (b < 0) return null;
        return xml.substring(a+open.length(), b).trim();
    }

}