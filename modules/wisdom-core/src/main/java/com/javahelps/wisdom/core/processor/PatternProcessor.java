package com.javahelps.wisdom.core.processor;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.pattern.Pattern;
import com.javahelps.wisdom.core.stream.Stream;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * The runtime {@link StreamProcessor} of {@link com.javahelps.wisdom.core.pattern.Pattern}s.
 */
public class PatternProcessor extends StreamProcessor {

    private Pattern firstPattern;
    private WisdomApp wisdomApp;
    private Map<Stream, CircularArray> patternMap = new HashMap<>();

    public PatternProcessor(String id, WisdomApp wisdomApp, Pattern firstPattern) {
        super(id, null);
        this.firstPattern = firstPattern;
        this.wisdomApp = wisdomApp;

        Pattern tempPattern = firstPattern;
        while (tempPattern != null) {
            CircularArray circularArray = this.patternMap.get(wisdomApp.getStream(tempPattern.getStreamId()));
            if (circularArray == null) {
                circularArray = new CircularArray();
                this.patternMap.put(wisdomApp.getStream(tempPattern.getStreamId()), circularArray);
            }
            circularArray.add(tempPattern);
            tempPattern = tempPattern.getNextPattern();
        }
    }

    @Override
    public void process(Event event) {
        this.patternMap.get(event.getStream()).process(event);
    }

    @Override
    public void process(Collection<Event> events) {

    }

    @Override
    public void setNextProcessor(Processor nextProcessor) {
        super.setNextProcessor(nextProcessor);
        Pattern pattern = this.firstPattern;
        while (pattern != null) {
            pattern.init(this.wisdomApp, nextProcessor);
            pattern = pattern.getNextPattern();
        }
    }

    private class CircularArray {

        private List<Pattern> patterns = new LinkedList<>();
        private int index;
        private int count;

        public void add(Pattern pattern) {
            this.patterns.add(pattern);
            this.count++;
        }

        public void process(Event event) {
            for (Pattern pattern : this.patterns) {
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
}
