package com.hcltech.rmg.appcontainer.interfaces;

import java.io.Serializable;

public record AppContainerDefn<EventSourceConfig, CepState, Msg, Schema, MetricsParam>(
        Class<IAppContainerFactory<EventSourceConfig, CepState, Msg, Schema, MetricsParam>> factoryClass,
        String containerId
) implements Serializable {
    public static <F extends IAppContainerFactory<EventSourceConfig, CepState, Msg, Schema, MetricsParam>, EventSourceConfig, CepState, Msg, Schema, MetricsParam> AppContainerDefn<EventSourceConfig, CepState, Msg, Schema, MetricsParam> of(
            Class<F> factoryClass,
            String containerId) {
        return new AppContainerDefn<>((Class) factoryClass, containerId);
    }


}
