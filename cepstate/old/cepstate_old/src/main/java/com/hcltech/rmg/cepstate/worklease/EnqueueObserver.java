package com.hcltech.rmg.cepstate.worklease;

@FunctionalInterface
interface EnqueueObserver<Msg> {
  void onEnqueued(String domainId, Msg message);
}
