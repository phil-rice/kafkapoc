package com.hcltech.rmg.execution.aspects;

import com.hcltech.rmg.common.HasType;
import com.hcltech.rmg.common.errorsor.ErrorsOr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry for a single aspect. Registers executors keyed by component type.
 * Each call to build(modules, aspect) returns a dispatcher (AspectExecutor) that routes by component.type()
 * against a point-in-time snapshot of current registrations.
 */
public final class AspectExecutorRepository<Component extends HasType, Inp, Out> {

    private static final Logger log = LoggerFactory.getLogger(AspectExecutorRepository.class);

    // Live registry; thread-safe for wiring at startup or runtime.
    private final Map<String, RegisteredAspectExecutor<Component, Inp, Out>> byType = new ConcurrentHashMap<>();

    /**
     * Register an executor under an exact type name. Duplicate registrations are rejected.
     */
    public AspectExecutorRepository<Component, Inp, Out> register(
            String typeName,
            RegisteredAspectExecutor<Component, Inp, Out> executor
    ) {
        Objects.requireNonNull(typeName, "typeName");
        Objects.requireNonNull(executor, "executor");
        if (typeName.isEmpty()) {
            throw new IllegalArgumentException("typeName must not be empty");
        }
        final var existing = byType.putIfAbsent(typeName, executor);
        if (existing != null) {
            throw new IllegalStateException("Executor already registered for type '" + typeName + "'");
        }
        log.info("Registered executor for type='{}'", typeName);
        return this;
    }

    /**
     * Register aliases that point to the same executor. Exact-match keys only.
     */
    public AspectExecutorRepository<Component, Inp, Out> alias(String existingType, String... aliases) {
        Objects.requireNonNull(existingType, "existingType");
        final var target = byType.get(existingType);
        if (target == null) {
            throw new IllegalArgumentException("No executor registered for '" + existingType + "' to alias");
        }
        for (String alias : aliases) {
            Objects.requireNonNull(alias, "alias");
            if (alias.isEmpty()) {
                throw new IllegalArgumentException("alias must not be empty");
            }
            final var prev = byType.putIfAbsent(alias, target);
            if (prev != null) {
                throw new IllegalStateException("Executor already registered for alias '" + alias + "'");
            }
            log.info("Registered alias '{}' -> '{}'", alias, existingType);
        }
        return this;
    }

    /**
     * Build a dispatcher bound to the provided modules and aspect.
     * The returned executor routes by component.type() and returns ErrorsOr<Out>.
     */
    public AspectExecutor<Component, Inp, Out> build(List<String> modules, String aspect) {
        if (aspect == null || aspect.isEmpty()) {
            throw new IllegalArgumentException("aspect must not be null or empty");
        }

        // Snapshot routing (keep insertion order for nicer logs).
        final Map<String, RegisteredAspectExecutor<Component, Inp, Out>> routing =
                Collections.unmodifiableMap(new LinkedHashMap<>(byType));

        final List<String> modulesSnapshot = (modules == null) ? List.of() : List.copyOf(modules);
        final String aspectSnapshot = aspect;

        log.info("Built dispatcher for aspect='{}' with {} executor(s): {}",
                aspectSnapshot, routing.size(), summarizeKeys(routing.keySet(), 20));

        return new BuiltAspectExecutor<>(routing, modulesSnapshot, aspectSnapshot);
    }

    /**
     * Optional: diagnostic helper for tests/ops.
     */
    public Set<String> getRegisteredTypes() {
        return Set.copyOf(byType.keySet());
    }

    private static String summarizeKeys(Collection<String> keys, int max) {
        if (keys.size() <= max) return keys.toString();
        final var it = keys.iterator();
        final List<String> first = new ArrayList<>(max);
        for (int i = 0; i < max && it.hasNext(); i++) first.add(it.next());
        return first + " … and " + (keys.size() - max) + " more";
    }

    /**
     * Private, snapshot-based dispatcher. Only the AspectExecutor interface is exposed to callers.
     */
    private static final class BuiltAspectExecutor<Component extends HasType, Inp, Out>
            implements AspectExecutor<Component, Inp, Out> {

        private static final Logger log = LoggerFactory.getLogger(BuiltAspectExecutor.class);

        private final Map<String, RegisteredAspectExecutor<Component, Inp, Out>> routing;
        private final List<String> modules;
        private final String aspect;

        BuiltAspectExecutor(
                Map<String, RegisteredAspectExecutor<Component, Inp, Out>> routing,
                List<String> modules,
                String aspect
        ) {
            this.routing = routing;
            this.modules = modules;
            this.aspect = aspect;
        }

        @Override
        public ErrorsOr<Out> execute(String key, Component component, Inp input) {
            if (component == null) {
                log.error("Dispatch failed: component is null (aspect='{}', modules={})", aspect, modules);
                return ErrorsOr.error("Component is null for aspect '" + aspect + "', modules=" + modules);
            }
            final String rawType = component.type();
            if (rawType == null || rawType.isEmpty()) {
                log.error("Dispatch failed: component.type() is null/empty (aspect='{}', modules={}, componentClass='{}')",
                        aspect, modules, component.getClass().getName());
                return ErrorsOr.error("Component type is null/empty for aspect '" + aspect + "', modules=" + modules);
            }

            final var exec = routing.get(rawType);
            if (exec == null) {
                log.error("Unknown component type '{}' (aspect='{}', modules={}). Known types={}",
                        rawType, aspect, modules, routing.keySet());
                return ErrorsOr.error(
                        "Unknown component type '" + rawType + "' for aspect '" + aspect +
                                "', modules=" + modules + ". Known types: " + routing.keySet()
                );
            }

            if (log.isDebugEnabled()) {
                log.debug("Dispatching (aspect='{}', type='{}', modules={}, componentClass='{}')",
                        aspect, rawType, modules, component.getClass().getName());
            }

            try {
                return exec.execute(key, modules, aspect, component, input);
            } catch (Exception ex) {
                // Defensive: registered executors should return ErrorsOr, but protect against throws.
                log.error("Executor threw (aspect='{}', type='{}', modules={}): {}",
                        aspect, rawType, modules, ex.toString(), ex);
                return ErrorsOr.error(
                        "Executor for type '" + rawType + "' threw: " +
                                ex.getClass().getSimpleName() + "/" + ex.getMessage()
                );
            }
        }
    }
}
