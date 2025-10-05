package com.hcltech.rmg.appcontainer.interfaces;

import com.hcltech.rmg.common.ITimeService;
import com.hcltech.rmg.common.uuid.IUuidGenerator;
import com.hcltech.rmg.config.config.Config;
import com.hcltech.rmg.config.config.RootConfig;
import com.hcltech.rmg.messages.IDomainTypeExtractor;
import com.hcltech.rmg.messages.IEventTypeExtractor;
import com.hcltech.rmg.parameters.ParameterExtractor;
import com.hcltech.rmg.xml.XmlTypeClass;

import java.util.List;
import java.util.Map;

public record AppContainer<EventSourceConfig, Schema>(
        // infra
        ITimeService time,
        IUuidGenerator uuid,

        // XML services
        XmlTypeClass<Schema> xml,                // keyExtraction + parse + validate

        // shared config
        List<String> keyPath,
        EventSourceConfig eventSourceConfig,
        RootConfig rootConfig,

        // getting things out of messages
        ParameterExtractor parameterExtractor,
        Map<String, Schema> nameToSchemaMap,
        IDomainTypeExtractor domainTypeExtractor,
        IEventTypeExtractor eventTypeExtractor,


        //The behaviour of the application. The key is the parameters key.
        Map<String, Config> keyToConfigMap
) {
}
