package com.hcltech.rmg.optics;

import com.hcltech.rmg.common.codec.Codec;
import org.apache.commons.jxpath.JXPathContext;

import java.util.List;
import java.util.Map;

public interface IOpticsEvent<Optics> {
    Optics apply(Optics initial);


    static <Child> IOpticsEvent<JXPathContext> setEvent(String path, Child value) {
        return new SetEvent<>(path, value);
    }

    static <Child> IOpticsEvent<JXPathContext> appendEvent(String path, Child value) {
        return new AppendEvent<>(path, value);
    }

    static Codec<IOpticsEvent<JXPathContext>, String> codec() {
        Codec<? extends IOpticsEvent<JXPathContext>, String> setC =
                (Codec<? extends IOpticsEvent<JXPathContext>, String>) (Codec<?, String>) Codec.clazzCodec(SetEvent.class);
        Codec<? extends IOpticsEvent<JXPathContext>, String> appendC =
                (Codec<? extends IOpticsEvent<JXPathContext>, String>) (Codec<?, String>) Codec.clazzCodec(AppendEvent.class);

        return Codec.<IOpticsEvent<JXPathContext>>polymorphicCodec(
                x -> {
                    if (x instanceof SetEvent) return "set";
                    if (x instanceof AppendEvent) return "append";
                    throw new IllegalArgumentException("Unknown event type: " + x.getClass());
                },
                Map.of(
                        "set", setC,
                        "append", appendC
                )

        );
    }
}

record SetEvent<Child>(String path, Child value) implements IOpticsEvent<JXPathContext> {
    @Override
    public JXPathContext apply(JXPathContext initial) {
        initial.setValue(path, value);
        return initial;
    }
}

record AppendEvent<Child>(String path, Child value) implements IOpticsEvent<JXPathContext> {
    @Override
    public JXPathContext apply(JXPathContext initial) {
        Object currentValue = initial.getValue(path);
        if (currentValue instanceof java.util.List<?>) {
            ((List<Child>) currentValue).add(value);
        } else {
            throw new IllegalArgumentException("Path does not point to a list: " + path);
        }
        return initial;
    }
}
