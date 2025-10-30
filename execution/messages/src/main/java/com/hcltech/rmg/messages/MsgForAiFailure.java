package com.hcltech.rmg.messages;

public record MsgForAiFailure<Msg>(
        Msg inputWithReference,
        Msg expectedOutput,
        Msg actualOutput
) {
}
