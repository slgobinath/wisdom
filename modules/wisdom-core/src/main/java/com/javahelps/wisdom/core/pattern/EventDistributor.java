package com.javahelps.wisdom.core.pattern;

import com.javahelps.wisdom.core.event.Event;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by gobinath on 7/4/17.
 */
class EventDistributor {

    private Map<String, List<Pattern>> patternsMap = new HashMap<>();

    public void add(Pattern pattern) {
        if (pattern == null) {
            return;
        }
        pattern.streamIds.forEach(id -> {
            List<Pattern> patterns = patternsMap.get(id);
            if (patterns == null) {
                patterns = new ArrayList<>();
                patternsMap.put(id, patterns);
            }
            patterns.add(pattern);
        });
    }

    public void process(Event event) {
        for (Pattern pattern : this.patternsMap.get(event.getStream().getId())) {
            if (pattern.isWaiting()) {
                pattern.process(event);
                if (!pattern.isWaiting()) {
                    // Consumed the event
                    break;
                }
            }
        }
    }
}
