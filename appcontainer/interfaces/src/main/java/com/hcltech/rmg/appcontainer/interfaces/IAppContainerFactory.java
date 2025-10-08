package com.hcltech.rmg.appcontainer.interfaces;

import ch.qos.logback.core.sift.AppenderFactory;
import com.hcltech.rmg.common.errorsor.ErrorsOr;

import java.util.concurrent.ConcurrentHashMap;

public interface IAppContainerFactory<EventSourceConfig, Msg, Schema> {
    ErrorsOr<AppContainer<EventSourceConfig, Msg, Schema>> create(String id);

    // factoryClass -> ErrorsOr<factory>
    static ConcurrentHashMap<Class<?>, ErrorsOr<? extends IAppContainerFactory<?, ?, ?>>> FACTORIES =
            new ConcurrentHashMap<>();

    // factoryClass -> (envId -> ErrorsOr<container>)
    static ConcurrentHashMap<Class<?>, ConcurrentHashMap<String, ErrorsOr<? extends AppContainer<?, ?, ?>>>> CONTAINERS =
            new ConcurrentHashMap<>();

    public static <ESC, M, S> ErrorsOr<IAppContainerFactory<ESC, M, S>> resolveFactory(Class<? extends IAppContainerFactory<ESC, M, S>> clazz) {
        var eo = FACTORIES.computeIfAbsent(clazz, c -> {
            try {
                return ErrorsOr.lift((IAppContainerFactory<?, ?, ?>) c.getDeclaredConstructor().newInstance());
            } catch (Exception e) {
                return ErrorsOr.error("Failed to instantiate container factory: {0} {1}", e);
            }
        });
        return castFactory(eo);
    }

    public static <ESC, M, S> ErrorsOr<AppContainer<ESC, M, S>> resolve(Class<? extends IAppContainerFactory<ESC, M, S>> clazz, String envId) {
        ErrorsOr<IAppContainerFactory<ESC, M, S>> errorsOrFactory = resolveFactory((Class) clazz);
        return errorsOrFactory.flatMap(factory -> {
            var byEnv = CONTAINERS.computeIfAbsent(clazz, _k -> new ConcurrentHashMap<>());
            var eo = byEnv.computeIfAbsent(envId, factory::create);
            return castContainer(eo);
        });
    }

    static <M, S>
    ErrorsOr<AppContainer<?, M, S>> resolveWithAnyMsc(
            Class<? extends IAppContainerFactory<?, M, S>> clazz,
            String envId
    ) {
        // reuse existing resolve(...) and normalize to "unknown ESC"
        var eo = IAppContainerFactory.resolve((Class) clazz, envId);
        @SuppressWarnings("unchecked")
        ErrorsOr<AppContainer<?, M, S>> out = (ErrorsOr<AppContainer<?, M, S>>) (ErrorsOr<?>) eo;
        return out;
    }

    // ---- single place for the only unchecked casts ----
    @SuppressWarnings("unchecked")
    private static <ESC, M, S> ErrorsOr<IAppContainerFactory<ESC, M, S>>
    castFactory(ErrorsOr<? extends IAppContainerFactory<?, ?, ?>> eo) {
        return (ErrorsOr<IAppContainerFactory<ESC, M, S>>) (ErrorsOr<?>) eo;
    }

    @SuppressWarnings("unchecked")
    private static <ESC, M, S> ErrorsOr<AppContainer<ESC, M, S>>
    castContainer(ErrorsOr<? extends AppContainer<?, ?, ?>> eo) {
        return (ErrorsOr<AppContainer<ESC, M, S>>) (ErrorsOr<?>) eo;
    }
}
