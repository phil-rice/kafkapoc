package com.hcltech.rmg.execution.aspects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry keyed by exact runtime component class.
 * Snapshot semantics: build() freezes current registrations.
 */
public final class AspectExecutorRepository<Component, Inp, Out> {

    private static final Logger log = LoggerFactory.getLogger(AspectExecutorRepository.class);

    // Live registry; thread-safe. Wildcard value so we can store base-/sub-typed executors.
    private final Map<Class<?>, AspectExecutor<?, Inp, Out>> byClass = new ConcurrentHashMap<>();

    /** Register an executor for an exact component class. Duplicate registrations are rejected. */
    public <T extends Component> AspectExecutorRepository<Component, Inp, Out> register(
            Class<T> componentClass,
            AspectExecutor<? super T, Inp, Out> executor
    ) {
        Objects.requireNonNull(componentClass, "componentClass");
        Objects.requireNonNull(executor, "executor");

        @SuppressWarnings("unchecked")
        AspectExecutor<?, Inp, Out> stored = (AspectExecutor<?, Inp, Out>) executor;

        var prev = byClass.putIfAbsent(componentClass, stored);
        if (prev != null) {
            throw new IllegalStateException("Executor already registered for class '" + componentClass.getName() + "'");
        }
        log.info("Registered executor for class='{}'", componentClass.getName());
        return this;
    }

    /**
     * Build a snapshot dispatcher. Later registrations don’t affect it.
     * The dispatcher routes by exact runtime class of the component.
     */
    public AspectExecutor<Component, Inp, Out> build() {
        final Map<Class<?>, AspectExecutor<?, Inp, Out>> routing =
                Collections.unmodifiableMap(new HashMap<>(byClass)); // snapshot copy

        log.info("Built dispatcher with {} executor(s): {}",
                routing.size(), summarize(routing.keySet(), 20));

        return new BuiltDispatcher<>(routing);
    }

    /** Inspect what’s registered at the moment (live view). */
    public Set<Class<?>> getRegisteredTypes() {
        return Set.copyOf(byClass.keySet());
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

    /** Snapshot-based dispatcher. */
    private static final class BuiltDispatcher<Component, Inp, Out> implements AspectExecutor<Component, Inp, Out> {
        private static final Logger log = LoggerFactory.getLogger(BuiltDispatcher.class);

        private final Map<Class<?>, AspectExecutor<?, Inp, Out>> routing;

        BuiltDispatcher(Map<Class<?>, AspectExecutor<?, Inp, Out>> routing) {
            this.routing = routing;
        }

        @Override
        public Out execute(String key, Component component, Inp input) {
            if (component == null) {
                log.error("Dispatch failed: component is null (key='{}')", key);
                throw new NullPointerException("Component is null for key '" + key + "'");
            }

            final Class<?> cls = component.getClass();
            final AspectExecutor<?, Inp, Out> exec = routing.get(cls);
            if (exec == null) {
                // Build a small “known classes” list without streams for speed
                var known = new ArrayList<String>(routing.size());
                for (Class<?> k : routing.keySet()) known.add(k.getName());

                log.error("Unknown component class '{}' (key='{}'). Known classes={}", cls.getName(), key, known);
                throw new IllegalStateException("Unknown component class '" + cls.getName()
                        + "' for key '" + key + "'. Known classes: " + known);
            }

            if (log.isDebugEnabled()) {
                log.debug("Dispatching key='{}', class='{}'", key, cls.getName());
            }

            // Single, centralized unchecked cast. Safe due to exact-class routing.
            @SuppressWarnings("unchecked")
            AspectExecutor<Component, Inp, Out> typed = (AspectExecutor<Component, Inp, Out>) exec;

            return typed.execute(key, component, input);
        }
    }
}
