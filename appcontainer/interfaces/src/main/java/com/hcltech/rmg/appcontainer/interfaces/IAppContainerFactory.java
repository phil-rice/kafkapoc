package com.hcltech.rmg.appcontainer.interfaces;

import com.hcltech.rmg.common.errorsor.ErrorsOr;

import java.util.concurrent.ConcurrentHashMap;

public interface IAppContainerFactory<EventSourceConfig, CepState, Msg, Schema, FlinkRT, FlinkFr, MetricsParam> {
    ErrorsOr<AppContainer<EventSourceConfig, CepState, Msg, Schema, FlinkRT, FlinkFr, MetricsParam>> create(String id);

    // factoryClass -> ErrorsOr<factory>
    static ConcurrentHashMap<Class<?>, ErrorsOr<? extends IAppContainerFactory<?, ?, ?, ?, ?, ?, ?>>> FACTORIES =
            new ConcurrentHashMap<>();

    // factoryClass -> (envId -> ErrorsOr<container>)
    static ConcurrentHashMap<Class<?>, ConcurrentHashMap<String, ErrorsOr<? extends AppContainer<?, ?, ?, ?, ?, ?, ?>>>> CONTAINERS =
            new ConcurrentHashMap<>();

    public static <ESC, C, M, S, RT, FR, MP> ErrorsOr<IAppContainerFactory<ESC, C, M, S, RT, FR, MP>> resolveFactory(Class<? extends IAppContainerFactory<ESC, C, M, S, RT, FR, MP>> clazz) {
        var eo = FACTORIES.computeIfAbsent(clazz, c -> {
            try {
                return ErrorsOr.lift((IAppContainerFactory<?, ?, ?, ?, ?, ?, ?>) c.getDeclaredConstructor().newInstance());
            } catch (Exception e) {
                return ErrorsOr.error("Failed to instantiate container factory: {0} {1}", e);
            }
        });
        return castFactory(eo);
    }

    public static <ESC, C, M, S, RT, FR, MP> ErrorsOr<AppContainer<ESC, C, M, S, RT, FR, MP>> resolve(AppContainerDefn<ESC, C, M, S, RT, FR, MP> defn) {
        Class<IAppContainerFactory<ESC, C, M, S, RT, FR, MP>> clazz = defn.factoryClass();
        ErrorsOr<IAppContainerFactory<ESC, C, M, S, RT, FR, MP>> errorsOrFactory = resolveFactory(clazz);
        return errorsOrFactory.flatMap(factory -> {
            var byEnv = CONTAINERS.computeIfAbsent(clazz, _k -> new ConcurrentHashMap<>());
            var eo = byEnv.computeIfAbsent(defn.containerId(), factory::create);
            return castContainer(eo);
        });
    }


    // ---- single place for the only unchecked casts ----
    @SuppressWarnings("unchecked")
    private static <ESC, C, M, S, RT, FR, MP> ErrorsOr<IAppContainerFactory<ESC, C, M, S, RT, FR, MP>>
    castFactory(ErrorsOr<? extends IAppContainerFactory<?, ?, ?, ?, ?, ?, ?>> eo) {
        return (ErrorsOr<IAppContainerFactory<ESC, C, M, S, RT, FR, MP>>) (ErrorsOr<?>) eo;
    }

    @SuppressWarnings("unchecked")
    private static <ESC, C, M, S, RT, FR, MP> ErrorsOr<AppContainer<ESC, C, M, S, RT, FR, MP>>
    castContainer(ErrorsOr<? extends AppContainer<?, ?, ?, ?, ?, ?, ?>> eo) {
        return (ErrorsOr<AppContainer<ESC, C, M, S, RT, FR, MP>>) (ErrorsOr<?>) eo;
    }
}
