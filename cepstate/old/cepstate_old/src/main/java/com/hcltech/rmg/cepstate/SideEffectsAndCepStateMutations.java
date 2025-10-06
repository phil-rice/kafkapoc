package com.hcltech.rmg.cepstate;

import com.hcltech.rmg.optics.IOpticsEvent;

import java.util.List;

public record SideEffectsAndCepStateMutations<Optics, S>(List<IOpticsEvent<Optics>> cepMutations,
                                                    List<S> sideEffects) {
}
