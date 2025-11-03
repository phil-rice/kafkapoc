package com.hcltech.rmg.worker.eventhubtest;

import net.jpountz.lz4.*;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * High-throughput LZ4 block compressor/decompressor for Flink DataStream pipelines.
 *
 * Design notes:
 *  - Per-subtask state (no ThreadLocal): Flink runs each RichFunction instance on a single task thread.
 *  - Reuse internal byte[] buffers and grow them exponentially to avoid per-record allocation.
 *  - Store original length in first 4 bytes (big-endian) before compressed payload.
 *  - Works with byte[] streams; helpers provided to map String<->byte[] using UTF-8 when needed.
 */
public final class Lz4CodecFunctions {

    /** Map<String,byte[]>: UTF-8 encode then LZ4 compress. */
    public static final class Utf8ToLz4Bytes extends RichMapFunction<String, byte[]> {
        private transient LZ4Compressor compressor;
        private transient byte[] work; // reusable output buffer

        @Override
        public void open(OpenContext parameters) {
            compressor = LZ4Factory.fastestInstance().fastCompressor();
            // start with a moderate buffer; it will grow as needed
            work = new byte[256 * 1024];
        }

        @Override
        public byte[] map(String value) {
            byte[] src = value.getBytes(StandardCharsets.UTF_8);
            return compressBytes(src);
        }

        private byte[] compressBytes(byte[] src) {
            final int srcLen = src.length;
            final int maxCompressed = compressor.maxCompressedLength(srcLen);
            int needed = 4 + maxCompressed;
            if (work.length < needed) {
                // grow ~2x to amortize realloc cost
                int cap = work.length;
                while (cap < needed) cap = cap < 1 ? 1 : Math.min(Integer.MAX_VALUE / 2, cap * 2);
                work = new byte[cap];
            }
            // write original length (big-endian)
            work[0] = (byte) (srcLen >>> 24);
            work[1] = (byte) (srcLen >>> 16);
            work[2] = (byte) (srcLen >>> 8);
            work[3] = (byte) (srcLen);
            int written = compressor.compress(src, 0, srcLen, work, 4, maxCompressed);
            return Arrays.copyOf(work, 4 + written); // trim to actual size
        }
    }

    /** Map<byte[],byte[]>: LZ4 compress raw bytes (already-encoded payload). */
    public static final class Lz4CompressBytes extends RichMapFunction<byte[], byte[]> {
        private transient LZ4Compressor compressor;
        private transient byte[] work;

        @Override
        public void open(OpenContext parameters) {
            compressor = LZ4Factory.fastestInstance().fastCompressor();
            work = new byte[256 * 1024];
        }

        @Override
        public byte[] map(byte[] value) {
            Preconditions.checkNotNull(value, "Input byte[] must not be null");
            final int srcLen = value.length;
            final int maxCompressed = compressor.maxCompressedLength(srcLen);
            int needed = 4 + maxCompressed;
            if (work.length < needed) {
                int cap = work.length;
                while (cap < needed) cap = cap < 1 ? 1 : Math.min(Integer.MAX_VALUE / 2, cap * 2);
                work = new byte[cap];
            }
            work[0] = (byte) (srcLen >>> 24);
            work[1] = (byte) (srcLen >>> 16);
            work[2] = (byte) (srcLen >>> 8);
            work[3] = (byte) (srcLen);
            int written = compressor.compress(value, 0, srcLen, work, 4, maxCompressed);
            return Arrays.copyOf(work, 4 + written);
        }
    }

    /** Map<byte[],byte[]>: LZ4 decompress raw bytes produced by the functions above. */
    public static final class Lz4DecompressBytes extends RichMapFunction<byte[], byte[]> {
        private transient LZ4FastDecompressor decompressor;
        private transient byte[] out;

        @Override
        public void open(OpenContext parameters) {
            decompressor = LZ4Factory.fastestInstance().fastDecompressor();
            out = new byte[256 * 1024];
        }

        @Override
        public byte[] map(byte[] value) {
            Preconditions.checkNotNull(value, "Input byte[] must not be null");
            Preconditions.checkArgument(value.length >= 4, "Corrupt LZ4 block: missing length header");
            // read original length (big-endian)
            int originalLen = ((value[0] & 0xFF) << 24)
                            | ((value[1] & 0xFF) << 16)
                            | ((value[2] & 0xFF) << 8)
                            |  (value[3] & 0xFF);
            Preconditions.checkArgument(originalLen >= 0, "Invalid length in LZ4 header: %s", originalLen);
            if (out.length < originalLen) {
                int cap = out.length;
                while (cap < originalLen) cap = cap < 1 ? 1 : Math.min(Integer.MAX_VALUE / 2, cap * 2);
                out = new byte[cap];
            }
            int compressedLen = value.length - 4;
            decompressor.decompress(value, 4, out, 0, originalLen);
            return Arrays.copyOf(out, originalLen);
        }
    }

    /** Map<byte[],String>: decode UTF-8 after decompression. */
    public static final class Lz4BytesToUtf8 extends RichMapFunction<byte[], String> {
        private final Lz4DecompressBytes delegate = new Lz4DecompressBytes();

        @Override public void open(OpenContext parameters) throws Exception { delegate.open(parameters); }
        @Override public String map(byte[] value) throws Exception {
            byte[] raw = delegate.map(value);
            return new String(raw, StandardCharsets.UTF_8);
        }
    }

    // --- Minimal pass-through (de)serializers for KafkaSource/Sink when using byte[] ---

    /** Pass-through serializer for KafkaSink<byte[]>. */
    public static final class ByteArraySerialization implements SerializationSchema<byte[]> {
        @Override public byte[] serialize(byte[] element) { return element; }
    }

    /** Pass-through deserializer for KafkaSource<byte[]> (value-only). */
    public static final class ByteArrayDeserialization extends AbstractSerializationlessByteArrayDeser {}

    /** Simple pass-through DeserializationSchema for byte[]. */
    static class AbstractSerializationlessByteArrayDeser extends AbstractDeserializationSchema<byte[]> {
        @Override public byte[] deserialize(byte[] message) throws IOException { return message; }
        @Override public boolean isEndOfStream(byte[] nextElement) { return false; }
        @Override
        @SuppressWarnings("unchecked")
        public org.apache.flink.api.common.typeinfo.TypeInformation<byte[]> getProducedType() {
            return (org.apache.flink.api.common.typeinfo.TypeInformation<byte[]>)
                    (org.apache.flink.api.common.typeinfo.Types.PRIMITIVE_ARRAY(
                            org.apache.flink.api.common.typeinfo.Types.BYTE));
        }
    }

    // --- Optional helpers if you want to compress/decompress using ByteBuffer instead of byte[] ---

    public static byte[] intToBigEndian(int n) {
        return new byte[]{ (byte)(n>>>24),(byte)(n>>>16),(byte)(n>>>8),(byte)n };
    }

    public static int readIntBE(byte[] a, int off) {
        return ((a[off] & 0xFF) << 24)
             | ((a[off+1] & 0xFF) << 16)
             | ((a[off+2] & 0xFF) << 8)
             |  (a[off+3] & 0xFF);
    }

    private Lz4CodecFunctions() {}
}
