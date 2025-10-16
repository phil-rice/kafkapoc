package com.hcltech.rmg.appcontainer.interfaces;

import com.hcltech.rmg.cepstate.CepStateTypeClass;
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

/**
 * Projection that exposes only what operators need, without the EventSourceConfig type.
 */
public interface InitialEnvelopeServices<CepState, Msg, Schema> {
    // infra
    ITimeService timeService();

    IUuidGenerator uuid();

    //CepState
    CepStateTypeClass<CepState> cepStateTypeClass();

    // XML services
    XmlTypeClass<Msg, Schema> xml();

    // shared config
    List<String> keyPath();

    RootConfig rootConfig();

    // message helpers
    ParameterExtractor<Msg> parameterExtractor();

    Map<String, Schema> nameToSchemaMap();

    IDomainTypeExtractor<Msg> domainTypeExtractor();

    IEventTypeExtractor<Msg> eventTypeExtractor();

    // behaviour/config per key
    Map<String, Config> keyToConfigMap();
}
