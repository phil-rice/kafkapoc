package com.hcltech.rmg.appcontainer.interfaces;

import java.io.Serializable;

public record AppContainerDefn<EventSourceConfig, CepState, Msg, Schema, FlinkRT,MetricsParam>(
        Class<IAppContainerFactory<EventSourceConfig, CepState, Msg, Schema,FlinkRT, MetricsParam>> factoryClass,
        String containerId
) implements Serializable {
    public static <F extends IAppContainerFactory<EventSourceConfig, CepState, Msg, Schema, FlinkRT,MetricsParam>, EventSourceConfig, CepState, Msg, Schema, FlinkRT,MetricsParam> AppContainerDefn<EventSourceConfig, CepState, Msg, Schema, FlinkRT,MetricsParam> of(
            Class<F> factoryClass,
            String containerId) {
        return new AppContainerDefn<>((Class) factoryClass, containerId);
    }


}
