package com.hcltech.rmg.execution.aspects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry keyed by exact runtime component class.
 * Snapshot semantics: build/buildSync freeze current registrations.
 * Supports both async-shaped and synchronous executors.
 */
public final class AspectExecutorRepository<Component, Inp, Out> {

    private static final Logger log = LoggerFactory.getLogger(AspectExecutorRepository.class);

    // Live registries; thread-safe.
    // Store with "? super Component" so the dispatcher avoids casts on hot path.
    private final Map<Class<?>, AspectExecutorAsync<? super Component, Inp, Out>> byClassAsync =
            new ConcurrentHashMap<>();

    private final Map<Class<?>, AspectExecutorSync<? super Component, Inp, Out>> byClassSync =
            new ConcurrentHashMap<>();

    /**
     * Register an ASYNC executor for an exact component class. Duplicate registrations are rejected.
     */
    public <T extends Component> AspectExecutorRepository<Component, Inp, Out> registerAsync(
            Class<T> componentClass,
            AspectExecutorAsync<? super T, Inp, Out> executor) {

        Objects.requireNonNull(componentClass, "componentClass");
        Objects.requireNonNull(executor, "executor");

        @SuppressWarnings("unchecked")
        AspectExecutorAsync<? super Component, Inp, Out> stored =
                (AspectExecutorAsync<? super Component, Inp, Out>) executor;

        var prev = byClassAsync.putIfAbsent(componentClass, stored);
        if (prev != null) {
            throw new IllegalStateException("Executor already registered for class '" + componentClass.getName() + "'");
        }

        if (log.isDebugEnabled()) {
            log.debug("Registered ASYNC executor for class='{}'", componentClass.getName());
        }
        return this;
    }

    /**
     * Register a SYNC executor for an exact component class. Duplicate registrations are rejected.
     * Also exposes the same executor via async shape using a boundary adapter.
     */
    public <T extends Component> AspectExecutorRepository<Component, Inp, Out> registerSync(
            Class<T> componentClass,
            AspectExecutorSync<? super T, Inp, Out> executor) {

        Objects.requireNonNull(componentClass, "componentClass");
        Objects.requireNonNull(executor, "executor");

        @SuppressWarnings("unchecked")
        AspectExecutorSync<? super Component, Inp, Out> storedSync =
                (AspectExecutorSync<? super Component, Inp, Out>) executor;

        var prevSync = byClassSync.putIfAbsent(componentClass, storedSync);
        if (prevSync != null) {
            throw new IllegalStateException("Executor already registered for class '" + componentClass.getName() + "'");
        }

        // Also register an async wrapper so async dispatch can use it if needed
        registerAsync(componentClass, new SyncToAsyncAspectExecutor<>(executor));

        if (log.isDebugEnabled()) {
            log.debug("Registered SYNC executor for class='{}'", componentClass.getName());
        }
        return this;
    }

    /**
     * Convenience alias for SYNC registration so callers can use repo.register(...).
     */
    public <T extends Component> AspectExecutorRepository<Component, Inp, Out> register(
            Class<T> componentClass,
            AspectExecutorSync<? super T, Inp, Out> executor) {
        return registerSync(componentClass, executor);
    }

    /**
     * Build an ASYNC snapshot dispatcher. Later registrations don’t affect it.
     */
    public AspectExecutorAsync<Component, Inp, Out> build() {
        final Map<Class<?>, AspectExecutorAsync<? super Component, Inp, Out>> routing =
                Collections.unmodifiableMap(new IdentityHashMap<>(byClassAsync)); // snapshot

        if (log.isDebugEnabled()) {
            log.debug("Built async dispatcher with {} executor(s): {}",
                    routing.size(), summarize(routing.keySet(), 20));
        } else {
            log.info("Built async dispatcher with {} executor(s)", routing.size());
        }

        return new BuiltAsyncDispatcher<>(routing);
    }

    /**
     * Build a SYNC snapshot dispatcher. Only classes registered via registerSync/register are routable.
     */
    public AspectExecutorSync<Component, Inp, Out> buildSync() {
        final Map<Class<?>, AspectExecutorSync<? super Component, Inp, Out>> routing =
                Collections.unmodifiableMap(new IdentityHashMap<>(byClassSync)); // snapshot

        if (log.isDebugEnabled()) {
            log.debug("Built sync dispatcher with {} executor(s): {}",
                    routing.size(), summarize(routing.keySet(), 20));
        } else {
            log.info("Built sync dispatcher with {} executor(s)", routing.size());
        }

        return new BuiltSyncDispatcher<>(routing);
    }

    public Set<Class<?>> getRegisteredTypes() {
        // union for diagnostics
        Set<Class<?>> s = new HashSet<>(byClassAsync.keySet());
        s.addAll(byClassSync.keySet());
        return Collections.unmodifiableSet(s);
    }

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

    // ---------- Dispatchers ----------

    private static final class BuiltAsyncDispatcher<Component, Inp, Out>
            implements AspectExecutorAsync<Component, Inp, Out> {

        private static final Logger log = LoggerFactory.getLogger(BuiltAsyncDispatcher.class);
        private final Map<Class<?>, AspectExecutorAsync<? super Component, Inp, Out>> routing;

        BuiltAsyncDispatcher(Map<Class<?>, AspectExecutorAsync<? super Component, Inp, Out>> routing) {
            this.routing = routing;
        }

        @Override
        public void call(String key, Component component, Inp input,
                         com.hcltech.rmg.common.function.Callback<? super Out> cb) {

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
                log.debug("Dispatching ASYNC key='{}', class='{}'", key, cls.getName());
            }

            try {
                exec.call(key, component, input, cb);
            } catch (Throwable t) {
                cb.failure(t);
            }
        }
    }

    private static final class BuiltSyncDispatcher<Component, Inp, Out>
            implements AspectExecutorSync<Component, Inp, Out> {

        private static final Logger log = LoggerFactory.getLogger(BuiltSyncDispatcher.class);
        private final Map<Class<?>, AspectExecutorSync<? super Component, Inp, Out>> routing;

        BuiltSyncDispatcher(Map<Class<?>, AspectExecutorSync<? super Component, Inp, Out>> routing) {
            this.routing = routing;
        }

        @Override
        public Out execute(String key, Component component, Inp input) {
            if (component == null) {
                log.error("Dispatch failed: component is null (key='{}')", key);
                throw new NullPointerException("Component is null for key '" + key + "'");
            }

            final Class<?> cls = component.getClass();
            final AspectExecutorSync<? super Component, Inp, Out> exec = routing.get(cls);
            if (exec == null) {
                var known = new ArrayList<String>(routing.size());
                for (Class<?> k : routing.keySet()) known.add(k.getName());
                var msg = "Unknown component class '" + cls.getName()
                        + "' for key '" + key + "'. Known classes: " + known;
                log.error(msg);
                throw new IllegalStateException(msg);
            }

            if (log.isDebugEnabled()) {
                log.debug("Dispatching SYNC key='{}', class='{}'", key, cls.getName());
            }

            // Hot path: direct call; let implementor throw if needed
            return exec.execute(key, component, input);
        }
    }
}
