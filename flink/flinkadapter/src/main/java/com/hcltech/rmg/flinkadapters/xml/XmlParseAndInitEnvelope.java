package com.hcltech.rmg.flinkadapters.xml;

import com.hcltech.rmg.appcontainer.interfaces.IAppContainer;
import com.hcltech.rmg.common.errorsor.ErrorsOr;
import com.hcltech.rmg.kafkaconfig.KafkaConfig;
import com.hcltech.rmg.messages.EnvelopeHeader;
import com.hcltech.rmg.messages.EnvelopeHeaderFactory;
import com.hcltech.rmg.messages.RawMessage;
import com.hcltech.rmg.messages.ValueEnvelope;
import com.hcltech.rmg.xml.XmlTypeClass;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.InputStream;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * In:  (domainId, RawMessage)
 * Out: (domainId, RawMessage, parsedDoc)
 * <p>
 * Errors go to a side output via the provided errorSink.
 * We keep error handling flexible (no dependency on your ErrorEnvelope constructor).
 */
public class XmlParseAndValidateProcess<CEPState>
        extends ProcessFunction<Tuple2<String, RawMessage>, ValueEnvelope<CEPState, Map<String, Object>>> {

    private final IAppContainer<KafkaConfig> appContainer;
    private final String schemaName;
    private final OutputTag<?> errorsTag;
    private final BiConsumer<ProcessFunction<Tuple2<String, RawMessage>, Tuple3<String, RawMessage, Map<String, Object>>>.Context, Object> errorSink;

    private transient XmlTypeClass<Object> xml;
    private transient Object schema;

    /**
     * @param appContainer source of XmlTypeClass and other infra
     * @param schemaName   classpath resource for the XSD
     * @param errorsTag    side output tag (your stream will define concrete type)
     * @param errorSink    how to emit an error object to the side output. Keep it generic so you can pass whatever envelope you use.
     */
    @SuppressWarnings("unchecked")
    public XmlParseAndValidateProcess(IAppContainer<KafkaConfig> appContainer,
                                      String schemaName,
                                      OutputTag<?> errorsTag,
                                      BiConsumer<ProcessFunction<Tuple2<String, RawMessage>, Tuple3<String, RawMessage, Map<String, Object>>>.Context, Object> errorSink) {
        this.appContainer = appContainer;
        this.schemaName = schemaName;
        this.errorsTag = errorsTag;
        this.errorSink = errorSink;
    }

    @Override
    public void open(OpenContext parameters) throws Exception {
        this.xml = (XmlTypeClass<Object>) appContainer.xml();
        try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(schemaName)) {
            if (in == null) {
                throw new IllegalStateException("XSD not found on classpath: " + schemaName);
            }
            this.schema = xml.loadSchema(schemaName, in);
        }
    }

    @Override
    public void processElement(Tuple2<String, RawMessage> stringRawMessageTuple2, ProcessFunction<Tuple2<String, RawMessage>, ValueEnvelope<CEPState, Map<String, Object>>>.Context context, Collector<ValueEnvelope<CEPState, Map<String, Object>>> out) throws Exception {

        String domainId = stringRawMessageTuple2.f0;
        RawMessage raw = stringRawMessageTuple2.f1;

        ErrorsOr<Map<String, Object>> result = xml.parseAndValidate(raw.rawValue(), schema);
        if (result.isValue()) {
            var header = EnvelopeHeaderFactory.
            out.collect(Tuple3.of(domainId, raw, result.valueOrThrow()));
        } else {
            // Keep error emission flexible: you decide how to wrap/emit.
            // Example: pass an ErrorEnvelope you build externally.
            if (errorsTag != null && errorSink != null) {
                Object errorObj = /* build your error object here or pass via closure */
                        result; // (placeholder) emit the ErrorsOr itself if useful
                errorSink.accept(ctx, errorObj);
            }
        }
    }
}
