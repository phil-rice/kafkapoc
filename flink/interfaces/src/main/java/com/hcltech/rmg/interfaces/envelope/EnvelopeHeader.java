package com.hcltech.rmg.interfaces.envelope;

import com.hcltech.rmg.interfaces.cepstate.CEPStateTypeClass;

public record EnvelopeHeader<CEPState>(String domainId,
                                       String eventType,
                                       long eventStartTime,
                                       long startTime,
                                       long endTimeOrMinus1,
                                       String rawInput,
                                       CEPState cepStateorNull,
                                       String tokenOrNull)  {

}
