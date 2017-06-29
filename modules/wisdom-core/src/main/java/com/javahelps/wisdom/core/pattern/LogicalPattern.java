package com.javahelps.wisdom.core.pattern;

import com.javahelps.wisdom.core.event.Event;

import java.util.List;

/**
 * Created by gobinath on 6/29/17.
 */
public class LogicalPattern extends Pattern {

    private List<Pattern> patterns;

    public LogicalPattern(String patternId) {
        super(patternId);
    }

    @Override
    public void process(Event event) {
        super.process(event);
    }
}
