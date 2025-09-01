package com.hcltech.rmg.consumer.cepstate;

import org.apache.kafka.common.TopicPartition;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public final class PartitionDbManager implements AutoCloseable {
    static { RocksDB.loadLibrary(); }

    private final Path baseDir; // e.g. /data/state
    private final String groupId;
    private final Map<TopicPartition, Entry> open = new ConcurrentHashMap<>();

    private record Entry(RocksDB db, Options opts, Path path) {}

    public PartitionDbManager(Path baseDir, String groupId) {
        this.baseDir = baseDir;
        this.groupId = groupId;
    }

    public synchronized void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        for (TopicPartition tp : partitions) {
            open.computeIfAbsent(tp, t -> {
                try {
                    Path dir = pathFor(t);
                    Files.createDirectories(dir);
                    Options opts = new Options().setCreateIfMissing(true);
                    RocksDB db = RocksDB.open(opts, dir.toString());
                    return new Entry(db, opts, dir);
                } catch (IOException | RocksDBException e) {
                    throw new RuntimeException("Opening RocksDB for " + t + " failed", e);
                }
            });
        }
    }

    public synchronized void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        for (TopicPartition tp : partitions) {
            Entry e = open.remove(tp);
            if (e != null) {
                // Flush if you add WAL/flush options; simplest is just close:
                e.db().close();
                e.opts().close();
            }
        }
    }

    public RocksDB dbFor(TopicPartition tp) {
        Entry e = open.get(tp);
        if (e == null) throw new IllegalStateException("No DB open for " + tp);
        return e.db();
    }

    public Path pathFor(TopicPartition tp) {
        return baseDir
                .resolve(safe(groupId))
                .resolve(safe(tp.topic()))
                .resolve("partition-" + tp.partition());
    }

    private static String safe(String s) {
        return s.replaceAll("[^A-Za-z0-9._-]", "_");
    }

    @Override public synchronized void close() {
        for (Entry e : open.values()) {
            e.db().close();
            e.opts().close();
        }
        open.clear();
    }
}
