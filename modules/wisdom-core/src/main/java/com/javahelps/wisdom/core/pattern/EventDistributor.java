package com.javahelps.wisdom.core.pattern;

import com.javahelps.wisdom.core.event.Event;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by gobinath on 7/4/17.
 */
class EventDistributor {

    private Map<String, Set<Pattern>> patternsMap = new HashMap<>();
    private List<Pattern> patternList = new ArrayList<>();

    public void add(Pattern pattern) {
        if (pattern == null) {
            return;
        }
        pattern.streamIds.forEach(id -> {
            Set<Pattern> patterns = patternsMap.get(id);
            if (patterns == null) {
                patterns = new LinkedHashSet<>();
                patternsMap.put(id, patterns);
            }
            patterns.add(pattern);
        });
        this.patternList.add(pattern);
    }

    public void process(Event event) {
        for (Pattern pattern : this.patternsMap.get(event.getStream().getId())) {
            if (pattern.isAccepting()) {
                pattern.process(event);
                if (pattern.isConsumed()) {
                    // Consumed the event
                    break;
                }
            }
        }
    }
}
