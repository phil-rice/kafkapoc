package com.hcltech.rmg.interfaces.cepstate;

import com.hcltech.rmg.optics.IOpticsEvent;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface CEPStateTypeClass <CEPState>{
    CompletableFuture<CEPState> fetch(String domainId);
    CompletableFuture<Void> append(String domainId, List<IOpticsEvent<CEPState>> cepUpdateEvents);
}
