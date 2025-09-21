package com.hcltech.rmg.domainpipeline.lease;

public interface MessageTC<M>{
     String domainId(M message);
     String messageId(M message);
     long messageAcquireTime(M message);
}
