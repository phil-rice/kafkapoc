package com.hcltech.rmg.appcontainer.interfaces;

import com.hcltech.rmg.common.errorsor.ErrorsOr;

import java.util.concurrent.ConcurrentHashMap;

public interface IAppContainerFactory<EventSourceConfig, CepState, Msg, Schema, MetricsParam> {
    ErrorsOr<AppContainer<EventSourceConfig, CepState, Msg, Schema, MetricsParam>> create(String id);

    // factoryClass -> ErrorsOr<factory>
    static ConcurrentHashMap<Class<?>, ErrorsOr<? extends IAppContainerFactory<?, ?, ?, ?, ?>>> FACTORIES =
            new ConcurrentHashMap<>();

    // factoryClass -> (envId -> ErrorsOr<container>)
    static ConcurrentHashMap<Class<?>, ConcurrentHashMap<String, ErrorsOr<? extends AppContainer<?, ?, ?, ?, ?>>>> CONTAINERS =
            new ConcurrentHashMap<>();

    public static <ESC, C, M, S, MP> ErrorsOr<IAppContainerFactory<ESC, C, M, S, MP>> resolveFactory(Class<? extends IAppContainerFactory<ESC, C, M, S, MP>> clazz) {
        var eo = FACTORIES.computeIfAbsent(clazz, c -> {
            try {
                return ErrorsOr.lift((IAppContainerFactory<?, ?, ?, ?, ?>) c.getDeclaredConstructor().newInstance());
            } catch (Exception e) {
                return ErrorsOr.error("Failed to instantiate container factory: {0} {1}", e);
            }
        });
        return castFactory(eo);
    }

    public static <ESC, C, M, S, MP> ErrorsOr<AppContainer<ESC, C, M, S, MP>> resolve(Class<? extends IAppContainerFactory<ESC, C, M, S, MP>> clazz, String envId) {
        ErrorsOr<IAppContainerFactory<ESC, C, M, S, MP>> errorsOrFactory = resolveFactory((Class) clazz);
        return errorsOrFactory.flatMap(factory -> {
            var byEnv = CONTAINERS.computeIfAbsent(clazz, _k -> new ConcurrentHashMap<>());
            var eo = byEnv.computeIfAbsent(envId, factory::create);
            return castContainer(eo);
        });
    }


    // ---- single place for the only unchecked casts ----
    @SuppressWarnings("unchecked")
    private static <ESC, C, M, S, MP> ErrorsOr<IAppContainerFactory<ESC, C, M, S, MP>>
    castFactory(ErrorsOr<? extends IAppContainerFactory<?, ?, ?, ?, ?>> eo) {
        return (ErrorsOr<IAppContainerFactory<ESC, C, M, S, MP>>) (ErrorsOr<?>) eo;
    }

    @SuppressWarnings("unchecked")
    private static <ESC, C, M, S, MP> ErrorsOr<AppContainer<ESC, C, M, S, MP>>
    castContainer(ErrorsOr<? extends AppContainer<?, ?, ?, ?, ?>> eo) {
        return (ErrorsOr<AppContainer<ESC, C, M, S, MP>>) (ErrorsOr<?>) eo;
    }
}
