package com.hcltech.rmg.optics;

import org.apache.commons.jxpath.JXPathContext;

import java.util.List;


public interface Interpreter<Main, C> {

    Main lift(C start);

    C getFrom(Main main);
    Main apply(List<IOpticsEvent<Main>> events, Main start);

    static <C> Interpreter<JXPathContext, C> jxPathInterpreter() {
        return new JXPathInterpreter<C>();
    }
}

class JXPathInterpreter<C> implements Interpreter<JXPathContext, C> {
    @Override
    public JXPathContext lift(C start) {
        return JXPathContext.newContext(start);
    }

    @Override
    public JXPathContext apply(List<IOpticsEvent<JXPathContext>> events, JXPathContext start) {
        for (var event : events) {
            event.apply(start);
        }
        return start;
    }
    @Override
    public C getFrom(JXPathContext main) {
        return (C) main.getValue(".");
    }

}

