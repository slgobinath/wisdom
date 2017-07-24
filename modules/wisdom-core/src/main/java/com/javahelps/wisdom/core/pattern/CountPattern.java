package com.javahelps.wisdom.core.pattern;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.processor.Processor;
import com.javahelps.wisdom.core.util.Scheduler;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Created by gobinath on 6/29/17.
 */
class CountPattern extends CustomPattern {

    private Pattern pattern;
    private Scheduler scheduler;
    private Event previousEvent;
    private int minCount;
    private int maxCount;

    CountPattern(String patternId, Pattern pattern, int minCount, int maxCount) {

        super(patternId);

        this.pattern = pattern;
        this.minCount = minCount;
        this.maxCount = maxCount;

        this.pattern.setProcessConditionMet(event -> pattern.getEvents().size() < this.maxCount);
        this.pattern.setEmitConditionMet(event -> pattern.getEvents().size() >= this.minCount);
        this.pattern.setCopyEventAttributes((pattern1, src, destination) -> {
            for (Map.Entry<String, Comparable> entry : src.getData().entrySet()) {
                destination.set(pattern.name + "[" + pattern.getEvents().size() + "]." + entry.getKey(),
                        entry.getValue());
            }
        });

        Predicate<Event> predicate = event -> this.pattern.isWaiting();
        this.predicate = predicate;
    }

    @Override
    public void previousEventProcessed(Event event) {

        if (duration != null) {
            this.previousEvent = event;
            scheduler.schedule(duration, this::timeoutHappend);
        }
    }

    public void timeoutHappend(long timestamp) {

        if (!this.isWaiting()) {
            this.getNextProcessor().process(this.previousEvent);
        }
    }

    @Override
    public void init(WisdomApp wisdomApp) {

        this.pattern.init(wisdomApp);
        this.scheduler = wisdomApp.getWisdomContext().getScheduler();
    }

    @Override
    public void setNextProcessor(Processor nextProcessor) {

        super.setNextProcessor(nextProcessor);
        this.pattern.setNextProcessor(nextProcessor);
    }

    @Override
    public void process(Event event) {

        this.pattern.process(event);
    }


    @Override
    public List<Event> getEvents() {
        return this.pattern.getEvents();
    }

    @Override
    public void setProcessConditionMet(Predicate<Event> processConditionMet) {

        this.pattern.setProcessConditionMet(processConditionMet);
    }

    @Override
    public void setEmitConditionMet(Predicate<Event> emitConditionMet) {

        Predicate<Event> predicate = this.predicate.and(emitConditionMet);
        this.pattern.setEmitConditionMet(predicate);
    }

    @Override
    public boolean isWaiting() {

        return pattern.getEvents().size() < this.minCount;
    }

    @Override
    public void reset() {
        this.pattern.reset();
    }

    @Override
    public void setMergePreviousEvents(Consumer<Event> mergePreviousEvents) {

        super.setMergePreviousEvents(mergePreviousEvents);
        this.pattern.setMergePreviousEvents(this.pattern.getMergePreviousEvents().andThen(mergePreviousEvents));
    }
}
