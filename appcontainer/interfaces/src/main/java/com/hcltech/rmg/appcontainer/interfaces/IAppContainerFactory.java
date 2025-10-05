package com.hcltech.rmg.appcontainer.interfaces;

import com.hcltech.rmg.common.errorsor.ErrorsOr;

/**
 * Factory that builds an AppContainer for a given environment “id”.
 */
public interface IAppContainerFactory<EventSourceConfig, Schema> {
    ErrorsOr<AppContainer<EventSourceConfig, Schema>> create(String id);
}
