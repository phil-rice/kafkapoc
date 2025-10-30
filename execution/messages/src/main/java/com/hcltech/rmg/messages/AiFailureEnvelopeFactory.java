package com.hcltech.rmg.messages;

import java.util.Map;

public interface AiFailureEnvelopeFactory<CepState, Msg> {

    AiFailureEnvelope<CepState, Msg> createAiFailureEnvelope(ValueEnvelope<CepState, Msg> valueEnvelope, Object actualProjection, Object expectedProjection);

    String BIZLOGIC_INPUT_CEP_STATE_CARGO_KEY = "bizlogicInputCepState";
    String BIZLOGIC_INPUT_MSG_CARGO_KEY = "bizlogicInputMsg";
    String BIZLOGIC_EXPECTED = "expected";


    static AiFailureEnvelopeFactory<Map<String, Object>, Map<String, Object>> fromValueEnvelope() {
        return (valueEnvelope, actualProjection, expectedProjection) -> {
            Object input = valueEnvelope.header().cargo().get(BIZLOGIC_INPUT_MSG_CARGO_KEY);
            Object cepState = valueEnvelope.header().cargo().get(BIZLOGIC_INPUT_CEP_STATE_CARGO_KEY);
            Object expected = valueEnvelope.header().cargo().get(BIZLOGIC_EXPECTED);
            Object actual = valueEnvelope.data();
            Map<String, Object> aiMsg = Map.of(
                    "input", input,
                    "cepState", cepState,
                    "actualProjection", actualProjection,
                    "expectedProjection", expectedProjection,
                    "actualOutput", actual,
                    "expectedOutput", expected
            );
            valueEnvelope.setData(aiMsg);
            return new AiFailureEnvelope<>(valueEnvelope);
        };
    }
}
