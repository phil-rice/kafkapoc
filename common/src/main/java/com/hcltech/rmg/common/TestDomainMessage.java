package com.hcltech.rmg.common;

import com.hcltech.rmg.common.codec.Codec;
import com.hcltech.rmg.common.errorsor.ErrorsOr;

public record TestDomainMessage(String domainId, String eventType, long count) {

    public static Codec<TestDomainMessage, String> xmlCodec = new Codec<TestDomainMessage, String>() {
        @Override
        public ErrorsOr<String> encode(TestDomainMessage testDomainMessage) {
            return ErrorsOr.lift("<msg><domainId>" + testDomainMessage.domainId + "</domainId><eventType>" + testDomainMessage.eventType + "</eventType><count>" + testDomainMessage.count + "</count></msg>");
        }

        @Override
        public ErrorsOr<TestDomainMessage> decode(String s) {
            try {
                var domainId = s.split("<domainId>")[1].split("</domainId>")[0];
                var eventType = s.split("<eventType>")[1].split("</eventType>")[0];
                var count = Long.parseLong(s.split("<count>")[1].split("</count>")[0]);
                return ErrorsOr.lift(new TestDomainMessage(domainId, eventType, count));
            } catch (Exception e) {
                return ErrorsOr.error("Failed to decode XML: " + e.getMessage());
            }
        }
    };

    public static TestDomainMessage create(String domainId, Long count) {
        return new TestDomainMessage(domainId, "test-event", count);
    }
}
