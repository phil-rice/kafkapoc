package com.example.optics;

import org.apache.commons.jxpath.JXPathContext;

import java.util.List;

public interface IOpticsEvent<Optics> {
    Optics apply(Optics initial);


    static <Child> IOpticsEvent<JXPathContext> setEvent(String path, Child value) {
        return new SetEvent<>(path, value);
    }

    static <Child> IOpticsEvent<JXPathContext> appendEvent(String path, Child value) {
        return new AppendEvent<>(path, value);
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