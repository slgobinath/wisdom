package com.javahelps.wisdom.core.pattern;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.processor.Processor;
import com.javahelps.wisdom.core.util.Scheduler;

import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Created by gobinath on 6/29/17.
 */
class NotPattern extends CustomPattern {

    private Pattern pattern;
    private Scheduler scheduler;
    private Event previousEvent;

    NotPattern(String patternId, Pattern pattern) {

        super(patternId);

        this.pattern = pattern;

        this.pattern.setProcessConditionMet(event -> true);

        this.pattern.setEvents(this.getEvents());

        Predicate<Event> predicate = event -> this.pattern.isWaiting();
        this.predicate = predicate;
    }

    @Override
    public void onPreviousPostProcess(Event event) {

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

        return !this.pattern.isWaiting();
    }

    @Override
    public void setMergePreviousEvents(Consumer<Event> mergePreviousEvents) {

        super.setMergePreviousEvents(mergePreviousEvents);
        this.pattern.setMergePreviousEvents(this.pattern.getMergePreviousEvents().andThen(mergePreviousEvents));
    }
}
