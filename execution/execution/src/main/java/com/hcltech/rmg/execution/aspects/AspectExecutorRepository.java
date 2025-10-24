package com.hcltech.rmg.execution.aspects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry keyed by exact runtime component class.
 * Snapshot semantics: build() freezes current registrations.
 * Dispatch is async-shaped via callbacks; sync impls may callback inline.
 */
public final class AspectExecutorRepository<Component, Inp, Out> {

    private static final Logger log = LoggerFactory.getLogger(AspectExecutorRepository.class);

    // Live registry; thread-safe. Store executors as "? super Component" so the dispatcher
    // doesn't need to cast on the hot path (improvement #5).
    private final Map<Class<?>, AspectExecutorAsync<? super Component, Inp, Out>> byClass =
            new ConcurrentHashMap<>();

    /**
     * Register an ASYNC executor for an exact component class. Duplicate registrations are rejected.
     */
    public <T extends Component> AspectExecutorRepository<Component, Inp, Out> registerAsync(
            Class<T> componentClass,
            AspectExecutorAsync<? super T, Inp, Out> executor) {

        Objects.requireNonNull(componentClass, "componentClass");
        Objects.requireNonNull(executor, "executor");

        // One-time unchecked cast at registration (keeps hot path clean of casts).
        @SuppressWarnings("unchecked")
        AspectExecutorAsync<? super Component, Inp, Out> stored =
                (AspectExecutorAsync<? super Component, Inp, Out>) executor;

        var prev = byClass.putIfAbsent(componentClass, stored);
        if (prev != null) {
            throw new IllegalStateException("Executor already registered for class '" + componentClass.getName() + "'");
        }

        // (#4) Reduce hot-path log noise: registration at DEBUG rather than INFO.
        if (log.isDebugEnabled()) {
            log.debug("Registered ASYNC executor for class='{}'", componentClass.getName());
        }
        return this;
    }

    /**
     * Convenience: register a SYNC executor; wrapped into async at the boundary.
     */
    public <T extends Component> AspectExecutorRepository<Component, Inp, Out> registerSync(
            Class<T> componentClass,
            AspectExecutor<? super T, Inp, Out> executor) {

        Objects.requireNonNull(componentClass, "componentClass");
        Objects.requireNonNull(executor, "executor");

        return registerAsync(componentClass, new SyncToAsyncAspectExecutor<>(executor));
    }

    /**
     * Build a snapshot dispatcher. Later registrations don’t affect it.
     */
    public AspectExecutorAsync<Component, Inp, Out> build() {
        final Map<Class<?>, AspectExecutorAsync<? super Component, Inp, Out>> routing =
                Collections.unmodifiableMap(new HashMap<>(byClass)); // snapshot copy

        // (#1) Guard expensive summary string creation behind log-level check.
        if (log.isDebugEnabled()) {
            log.debug("Built async dispatcher with {} executor(s): {}",
                    routing.size(), summarize(routing.keySet(), 20));
        } else {
            log.info("Built async dispatcher with {} executor(s)", routing.size());
        }

        return new BuiltDispatcher<>(routing);
    }

    public Set<Class<?>> getRegisteredTypes() { return Set.copyOf(byClass.keySet()); }

    private static String summarize(Collection<Class<?>> keys, int max) {
        if (keys.isEmpty()) return "[]";
        if (keys.size() <= max) {
            var list = new ArrayList<String>(keys.size());
            for (Class<?> k : keys) list.add(k.getName());
            return list.toString();
        }
        var it = keys.iterator();
        var first = new ArrayList<String>(max);
        for (int i = 0; i < max && it.hasNext(); i++) first.add(it.next().getName());
        return first + " … and " + (keys.size() - max) + " more";
    }

    /**
     * Snapshot-based dispatcher using exact-class routing.
     */
    private static final class BuiltDispatcher<Component, Inp, Out>
            implements AspectExecutorAsync<Component, Inp, Out> {

        private static final Logger log = LoggerFactory.getLogger(BuiltDispatcher.class);
        private final Map<Class<?>, AspectExecutorAsync<? super Component, Inp, Out>> routing;

        BuiltDispatcher(Map<Class<?>, AspectExecutorAsync<? super Component, Inp, Out>> routing) {
            this.routing = routing;
        }

        @Override
        public void call(String key, Component component, Inp input,
                         com.hcltech.rmg.common.function.Callback<? super Out> cb) {

            // (#3) Null-safety for callback.
            Objects.requireNonNull(cb, "callback");

            if (component == null) {
                log.error("Dispatch failed: component is null (key='{}')", key);
                cb.failure(new NullPointerException("Component is null for key '" + key + "'"));
                return;
            }

            final Class<?> cls = component.getClass();
            final AspectExecutorAsync<? super Component, Inp, Out> exec = routing.get(cls);
            if (exec == null) {
                var known = new ArrayList<String>(routing.size());
                for (Class<?> k : routing.keySet()) known.add(k.getName());
                var ex = new IllegalStateException("Unknown component class '" + cls.getName()
                        + "' for key '" + key + "'. Known classes: " + known);
                log.error(ex.getMessage());
                cb.failure(ex);
                return;
            }

            if (log.isDebugEnabled()) {
                log.debug("Dispatching key='{}', class='{}'", key, cls.getName());
            }

            // (#2) Catch executor throws in dispatcher, covering custom async impls that might throw.
            try {
                // Async-shaped call; sync impls may callback inline (#6: re-entrancy).
                exec.call(key, component, input, cb);
            } catch (Throwable t) {
                cb.failure(t);
            }
        }
    }
}
