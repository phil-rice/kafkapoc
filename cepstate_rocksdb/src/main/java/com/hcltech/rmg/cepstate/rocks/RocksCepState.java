package com.hcltech.rmg.cepstate.rocks;

import com.hcltech.rmg.cepstate.CepState;
import com.hcltech.rmg.common.Codec;
import com.hcltech.rmg.optics.IOpticsEvent;
import com.hcltech.rmg.optics.Interpreter;
import org.rocksdb.*;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Event-sourced CepState backed by RocksDB.
 * <p>
 * Layout:
 * - Per-domain event batches appended as:  evt:<len><domainId><seq(8B BE)>  -> codec.encode(List<OpticsEvent<C>>)
 * - Last sequence number tracked at:       seq:<len><domainId>              -> 8B BE long
 * <p>
 * Reads:
 * - Iterate only this domain's event range (bounded), decode each batch, and fold into C via the Interpreter.
 * <p>
 * Writes:
 * - Append one encoded batch at the next sequence and update seq:<id> in the same WriteBatch.
 * <p>
 * DB lifecycle (open/close) is owned by the caller.
 *
 * @param eventCodec Codec for persisting a batch of events: List<OpticsEvent<C>> <-> byte[]
 */
public record RocksCepState<Optics, C>(RocksDB db, Codec<List<IOpticsEvent<Optics>>, byte[]> eventCodec,
                                       Interpreter<Optics, C> interpreter) implements CepState<Optics,C> {

    private static final byte[] NS_EVT = "evt:".getBytes(StandardCharsets.UTF_8);
    private static final byte[] NS_SEQ = "seq:".getBytes(StandardCharsets.UTF_8);

    public RocksCepState(RocksDB db,
                         Codec<List<IOpticsEvent<Optics>>, byte[]> eventCodec,
                         Interpreter<Optics, C> interpreter) {
        this.db = Objects.requireNonNull(db, "db");
        this.eventCodec = Objects.requireNonNull(eventCodec, "eventCodec");
        this.interpreter = Objects.requireNonNull(interpreter, "interpreter");
    }

    @Override
    public CompletionStage<C> get(String domainId, C defaultValue) {
        final byte[] prefix = evtPrefix(domainId);
        final byte[] upper = upperBound(prefix); // exclusive upper bound for this domain range

        try (Slice ub = new Slice(upper);
             ReadOptions ro = new ReadOptions().setIterateUpperBound(ub);
             RocksIterator it = db.newIterator(ro)) {

            it.seek(prefix);

            var state = interpreter.lift(defaultValue);
            while (it.isValid()) {
                // Each value is a batch (List<OpticsEvent<C>>) for a single sequence
                List<IOpticsEvent<Optics>> batch = eventCodec.decode(it.value());
                if (!batch.isEmpty()) {
                    state = interpreter.apply(batch, state); // fold this batch only
                }
                it.next();
            }
            return CompletableFuture.completedFuture(interpreter.getFrom(state));
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public CompletionStage<Void> mutate(String domainId, List<IOpticsEvent<Optics>> mutations) {
        if (mutations == null || mutations.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        try (WriteBatch wb = new WriteBatch(); WriteOptions wo = new WriteOptions()) {
            long last = readLastSeq(domainId); //bigendian long
            long next = last + 1;

            // store this batch at the next sequence
            wb.put(evtKey(domainId, next), eventCodec.encode(mutations));
            // persist last sequence (next) atomically with the batch append
            wb.put(seqKey(domainId), longBE(next));

            db.write(wo, wb);
            return CompletableFuture.completedFuture(null);
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    // -------- key encoding / helpers --------

    private static byte[] evtPrefix(String domainId) {
        byte[] id = domainId.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(NS_EVT.length + 4 + id.length);
        buf.put(NS_EVT).putInt(id.length).put(id);
        return buf.array();
    }

    private static byte[] evtKey(String domainId, long seq) {
        byte[] prefix = evtPrefix(domainId);
        byte[] out = new byte[prefix.length + 8];
        System.arraycopy(prefix, 0, out, 0, prefix.length);
        putLongBE(out, prefix.length, seq);
        return out;
    }

    private static byte[] seqKey(String domainId) {
        byte[] id = domainId.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(NS_SEQ.length + 4 + id.length);
        buf.put(NS_SEQ).putInt(id.length).put(id);
        return buf.array();
    }

    private long readLastSeq(String domainId) throws RocksDBException {
        byte[] b = db.get(seqKey(domainId));
        return (b == null) ? -1L : ByteBuffer.wrap(b).getLong();
    }

    private static byte[] longBE(long v) {
        return ByteBuffer.allocate(8).putLong(v).array();
    }

    private static void putLongBE(byte[] arr, int off, long v) {
        for (int i = 7; i >= 0; i--) {
            arr[off + i] = (byte) (v & 0xFF);
            v >>>= 8;
        }
    }

    /**
     * Exclusive upper bound: prefix + 0xFF so the iterator won't step into the next domain's keys.
     */
    private static byte[] upperBound(byte[] prefix) {
        byte[] u = new byte[prefix.length + 1];
        System.arraycopy(prefix, 0, u, 0, prefix.length);
        u[prefix.length] = (byte) 0xFF;
        return u;
    }

}
