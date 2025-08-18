package com.example.cepstate;

import com.example.optics.IOpticsEvent;

import java.util.List;

public record SideEffectsAndCepStateMutations<Optics, S>(List<IOpticsEvent<Optics>> cepMutations,
                                                    List<S> sideEffects) {
}
