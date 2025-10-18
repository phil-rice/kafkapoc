package com.hcltech.rmg.kafka;

import com.hcltech.rmg.messages.ErrorEnvelope;
import com.hcltech.rmg.messages.RetryEnvelope;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.OutputTag;

public interface EnvelopeOutputTags {

    OutputTag<ErrorEnvelope<?, ?>> ERRORS =
            new OutputTag<>("errors",
                    TypeInformation.of(new TypeHint<ErrorEnvelope<?, ?>>() {
                    })) {
            };

    OutputTag<RetryEnvelope<?, ?>> RETRIES =
            new OutputTag<>("retries",
                    TypeInformation.of(new TypeHint<RetryEnvelope<?, ?>>() {
                    })) {
            };
}
