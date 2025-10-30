package com.hcltech.rmg.common;

import com.hcltech.rmg.common.codec.Codec;
import com.hcltech.rmg.common.errorsor.ErrorsOr;

/**
 * Simple domain message used by the perf tools.
 *
 * XML shape:
 * <msg>
 *   <domainId>...</domainId>
 *   <eventType>...</eventType>
 *   <productType>...</productType>
 *   <company>...</company>
 *   <count>...</count>
 * </msg>
 */
public record TestDomainMessage(
        String domainId,
        String eventType,
        String productType,
        String company,
        long count
) {

    public static final Codec<TestDomainMessage, String> xmlCodec = new Codec<>() {

        @Override
        public ErrorsOr<String> encode(TestDomainMessage m) {
            // Minimal XML serializer (no namespaces/escaping). Fine for test harness payloads.
            String xml = "<msg>"
                    + tag("domainId",   m.domainId)
                    + tag("eventType",  m.eventType)
                    + tag("productType", m.productType)
                    + tag("company",     m.company)
                    + tag("count",       Long.toString(m.count))
                    + "</msg>";
            return ErrorsOr.lift(xml);
        }

        @Override
        public ErrorsOr<TestDomainMessage> decode(String s) {
            try {
                String domainId    = getTag(s, "domainId");
                String eventType   = getTag(s, "eventType");
                String productType = getTag(s, "productType");
                String company     = getTag(s, "company");
                String countStr    = getTag(s, "count");

                if (domainId == null || eventType == null || productType == null || company == null || countStr == null) {
                    return ErrorsOr.error("Failed to decode XML: missing required fields");
                }

                long count = Long.parseLong(countStr.trim());
                return ErrorsOr.lift(new TestDomainMessage(domainId, eventType, productType, company, count));
            } catch (Exception e) {
                return ErrorsOr.error("Failed to decode XML: " + e.getMessage());
            }
        }

        private String tag(String name, String value) {
            return "<" + name + ">" + (value == null ? "" : value) + "</" + name + ">";
        }

        private String getTag(String xml, String name) {
            String open = "<" + name + ">";
            String close = "</" + name + ">";
            int i = xml.indexOf(open);
            if (i < 0) return null;
            int j = xml.indexOf(close, i + open.length());
            if (j < 0) return null;
            return xml.substring(i + open.length(), j);
        }
    };

    /** Convenience creator with defaults for perf tests. */
    public static TestDomainMessage create(String domainId, Long count) {
        // keep the old behavior but fill the new fields with harmless defaults
        return new TestDomainMessage(domainId, "test-event", "parcel", "rmg", count == null ? 0L : count);
    }

    /** Overload: specify all fields explicitly. */
    public static TestDomainMessage create(String domainId, String eventType, String productType, String company, long count) {
        return new TestDomainMessage(domainId, eventType, productType, company, count);
    }
}
