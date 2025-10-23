package com.hcltech.rmg.config.ai;

import com.hcltech.rmg.config.aspect.AspectMap;
import com.hcltech.rmg.config.bizlogic.BizLogicAspect;
import com.hcltech.rmg.config.bizlogic.CelInlineLogic;
import com.hcltech.rmg.config.config.BehaviorConfig;
import com.hcltech.rmg.config.config.Config;
import com.hcltech.rmg.config.configs.Configs;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Maps the special AI JSON format (AiIncomingPayload) into canonical Configs.
 */
public final class AiPayloadToConfigs {

    private AiPayloadToConfigs() {}

    /**
     * Convert AI-format payload to Configs (key -> Config).
     *
     * @param payload AI-format payload (already deserialized)
     * @return Configs containing one Config per (paramKey,eventName,moduleName) in bizlogic
     */
    public static Configs toConfigs(AiIncomingPayload payload) {
        Objects.requireNonNull(payload, "payload");
        var root = payload.rootConfig();
        Objects.requireNonNull(root, "root");

        final String celForAi = payload.celProjection();
        final Map<String, Config> out = new LinkedHashMap<>();

        // We want: ONE Config per paramKey.
        // For each paramKey, collect bizlogic modules grouped by eventName,
        // then materialize a BehaviorConfig { eventName -> AspectMap(bizlogic=modules...) }.
        for (Map.Entry<String, AiParameterConfigBlock> paramEntry
                : payload.config().byParameterKey().entrySet()) {

            final String paramKey = paramEntry.getKey();
            final AiEvents eventsWrapper = paramEntry.getValue().events();
            if (eventsWrapper == null) continue;

            // eventName -> (moduleName -> CelInlineLogic)
            final Map<String, Map<String, BizLogicAspect>> bizlogicByEvent = new LinkedHashMap<>();

            for (Map.Entry<String, AiEventDef> eventEntry : eventsWrapper.byEvent().entrySet()) {
                final String eventName = eventEntry.getKey();
                final Map<String, Map<String, CelInlineLogic>> byAspect = eventEntry.getValue().byAspect();

                // Only bizlogic (string key form)
                final Map<String, CelInlineLogic> modules =
                        byAspect.getOrDefault(BehaviorConfig.bizlogicAspectName, Map.of());
                if (modules.isEmpty()) continue;

                // Merge modules for this event into the accumulator
                bizlogicByEvent
                        .computeIfAbsent(eventName, __ -> new LinkedHashMap<>())
                        .putAll(modules);
            }

            // If no bizlogic at all for this paramKey, skip emitting a Config
            if (bizlogicByEvent.isEmpty()) continue;

            // Build BehaviorConfig: for each event, create an AspectMap with only bizlogic populated
            final Map<String, AspectMap> behaviorEvents = new LinkedHashMap<>();
            for (Map.Entry<String, Map<String, BizLogicAspect>> e : bizlogicByEvent.entrySet()) {
                final String eventName = e.getKey();
                final Map<String, BizLogicAspect> modules = e.getValue();

                AspectMap aspects = new AspectMap(
                        Map.of(),               // validation
                        Map.of(),               // transformation
                        Map.of(),               // enrichment
                        modules                 // bizlogic
                );
                behaviorEvents.put(eventName, aspects);
            }

            BehaviorConfig behavior = new BehaviorConfig(behaviorEvents);

            // Compose canonical config for THIS paramKey only
            Config config = new Config(
                    behavior,
                    root.parameterConfig(),
                    root.xmlSchemaPath(),
                    celForAi
            );

            // 🔑 Key is JUST the parameter key now (e.g. "parcel:rmg")
            out.put(paramKey, config);
        }

        return new Configs(out);
    }

}
