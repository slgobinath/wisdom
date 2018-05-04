package com.javahelps.wisdom.core.pattern;

import com.javahelps.wisdom.core.event.Event;

import java.util.*;

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
