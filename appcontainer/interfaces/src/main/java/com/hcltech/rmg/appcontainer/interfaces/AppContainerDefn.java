package com.hcltech.rmg.appcontainer.interfaces;

import java.io.Serializable;

public record AppContainerDefn<EventSourceConfig, CepState, Msg, Schema, FlinkRT,FlinkFR,MetricsParam>(
        Class<IAppContainerFactory<EventSourceConfig, CepState, Msg, Schema,FlinkRT,FlinkFR, MetricsParam>> factoryClass,
        String containerId
) implements Serializable {
    public static <F extends IAppContainerFactory<EventSourceConfig, CepState, Msg, Schema, FlinkRT,FlinkFR,MetricsParam>, EventSourceConfig, CepState, Msg, Schema, FlinkRT,FlinkFR,MetricsParam> AppContainerDefn<EventSourceConfig, CepState, Msg, Schema, FlinkRT,FlinkFR,MetricsParam> of(
            Class<F> factoryClass,
            String containerId) {
        return new AppContainerDefn<>((Class) factoryClass, containerId);
    }


}
