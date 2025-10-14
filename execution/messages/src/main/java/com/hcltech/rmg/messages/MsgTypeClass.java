package com.hcltech.rmg.messages;

public interface MsgTypeClass<Msg, P> {
    Object getValueFromPath(Msg msg, P path);

}

