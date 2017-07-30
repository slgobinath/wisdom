package com.javahelps.wisdom.core.pattern;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.processor.Processor;
import com.javahelps.wisdom.core.util.Scheduler;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Created by gobinath on 6/29/17.
 */
class NotPattern extends CustomPattern implements EmptiablePattern {

    private static final List<Event> EVENT_LIST = Arrays.asList(new Event(0));
    private Pattern pattern;
    private Scheduler scheduler;
    private Event previousEvent;

    NotPattern(String patternId, Pattern pattern) {

        super(patternId);

        this.pattern = pattern;

        this.pattern.setProcessConditionMet(event -> true);

//        this.pattern.setEvents(this.getEvents());

//        Predicate<Event> predicate = event -> this.pattern.isConsumed();
//        this.predicate = predicate;
        this.streamIds.addAll(this.pattern.streamIds);
    }

    @Override
    public void onPreviousPostProcess(Event event) {

        if (duration != null) {
            this.previousEvent = event;
            scheduler.schedule(duration, this::timeoutHappend);
        }
    }

    public void timeoutHappend(long timestamp) {

        if (this.pattern.isAccepting()) {
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
    public void setProcessConditionMet(Predicate<Event> processConditionMet) {

        this.pattern.setProcessConditionMet(processConditionMet);
    }

    @Override
    public void setEmitConditionMet(Predicate<Event> emitConditionMet) {

        Predicate<Event> predicate = this.predicate.and(emitConditionMet);
        this.pattern.setEmitConditionMet(predicate);
    }

    @Override
    public boolean isAccepting() {
        return this.pattern.isAccepting();
    }

    @Override
    public boolean isConsumed() {

        return this.pattern.isConsumed();
    }

    @Override
    public void setConsumed(boolean consumed) {
        this.pattern.setConsumed(consumed);
    }

    @Override
    public boolean isComplete() {

        return !this.pattern.isComplete();
    }

//    @Override
//    public void setMergePreviousEvents(Consumer<Event> mergePreviousEvents) {
//
//        super.setMergePreviousEvents(mergePreviousEvents);
//        this.pattern.setMergePreviousEvents(this.pattern.getMergePreviousEvents().andThen(mergePreviousEvents));
//    }


    @Override
    public void setPreviousEvents(Supplier<List<Event>> previousEvents) {
        super.setPreviousEvents(previousEvents);
        this.pattern.setPreviousEvents(previousEvents);
    }

    @Override
    public List<Event> getEvents() {
        return this.getEvents(true);
    }

    @Override
    public List<Event> getEvents(boolean isFirst) {

        List<Event> events = new ArrayList<>();
        if (this.pattern.getEvents().isEmpty()) {
            if (isFirst) {
                events.add(EmptiablePattern.EMPTY_EVENT);
            } else {
                events = this.getPreviousEvents().get();
            }
        } else {
            return Collections.EMPTY_LIST;
        }
        return events;
    }
}
