package com.hcltech.rmg.cepstate.worklease;

import java.io.Serializable;

public record MsgAndToken<Msg>(Msg message, String token) implements Serializable {}