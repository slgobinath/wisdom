package com.javahelps.wisdom.core.pattern;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.processor.Processor;

import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Created by gobinath on 6/29/17.
 */
class EveryPattern extends CustomPattern {

    private Pattern pattern;

    EveryPattern(String patternId, Pattern pattern) {

        super(patternId);

        this.pattern = pattern;
        this.pattern.setBatchPattern(true);
        this.setBatchPattern(true);
        this.pattern.setProcessConditionMet(event -> true);

        this.streamIds.addAll(this.pattern.streamIds);
    }

    @Override
    public void init(WisdomApp wisdomApp) {

        this.pattern.init(wisdomApp);
        this.pattern.streamIds.forEach(streamId -> {
            wisdomApp.getStream(streamId).removeProcessor(this.pattern);
            wisdomApp.getStream(streamId).addProcessor(this);
        });
    }

    @Override
    public void setNextProcessor(Processor nextProcessor) {

        super.setNextProcessor(nextProcessor);
        this.pattern.setNextProcessor(nextProcessor);
    }

    @Override
    public void process(Event event) {

        try {
            this.lock.lock();
            this.pattern.process(event);
            this.pattern.setAccepting(true);
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public boolean isConsumed() {
        return this.pattern.isConsumed();
    }

    @Override
    public void setConsumed(boolean consumed) {
        this.pattern.setConsumed(false);
    }

    @Override
    public boolean isComplete() {
        return this.pattern.isComplete();
    }

    @Override
    public boolean isAccepting() {
        return true;
    }

    @Override
    public List<Event> getEvents() {
        return this.pattern.getEvents();
    }

    @Override
    public void setProcessConditionMet(Predicate<Event> processConditionMet) {

//        processConditionMet = processConditionMet.or(event -> !this.getEvents().isEmpty());
//        this.pattern.setProcessConditionMet(this.pattern.getProcessConditionMet().and(processConditionMet));
        this.pattern.setProcessConditionMet(processConditionMet);
    }

    @Override
    public void setEmitConditionMet(Predicate<Event> emitConditionMet) {

//        Predicate<Event> predicate = this.predicate.and(emitConditionMet);
        this.pattern.setEmitConditionMet(emitConditionMet);
    }

    @Override
    public void reset() {
        this.pattern.reset();
    }

//    @Override
//    public void setMergePreviousEvents(Consumer<Event> mergePreviousEvents) {
//
//        super.setMergePreviousEvents(mergePreviousEvents);
//        this.pattern.setMergePreviousEvents(this.pattern.getMergePreviousEvents().andThen(mergePreviousEvents));
//    }

    @Override
    public void setPreviousEvents(Supplier<List<Event>> previousEvents) {
        this.pattern.setPreviousEvents(previousEvents);
    }

    @Override
    public void onNextPreProcess(Event event) {
        super.onNextPreProcess(event);
    }

    @Override
    public void clear() {
        try {
            this.lock.lock();
            this.pattern.clear();
        } finally {
            this.lock.unlock();
        }
    }
}
