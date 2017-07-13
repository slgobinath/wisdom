package com.javahelps.wisdom.core.pattern;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.processor.Processor;

import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Created by gobinath on 6/29/17.
 */
class LogicalPattern extends CustomPattern {

    public enum Type {
        OR, AND
    }


    private Type type;
    private Pattern patternX;
    private Pattern patternY;

    LogicalPattern(String patternId, Type type, Pattern patternX, Pattern patternY) {

        super(patternId);
        this.type = type;

        this.patternX = patternX;
        this.patternY = patternY;
        this.eventDistributor.add(patternX);
        this.eventDistributor.add(patternY);

        this.patternX.setProcessConditionMet(event -> true);
        this.patternY.setProcessConditionMet(event -> true);

        this.patternX.setEvents(this.getEvents());
        this.patternX.setEvents(this.getEvents());

        Predicate<Event> predicate;
        if (type == Type.AND) {
            predicate = event -> !this.patternX.isWaiting() && !this.patternY.isWaiting();
        } else {
            // OR
            predicate = event -> !this.patternX.isWaiting() || !this.patternY.isWaiting();
        }
        this.predicate = predicate;

        this.patternX.setMergePreviousEvents(event -> {
            for (Event e : this.patternY.getEvents()) {
                event.getData().putAll(e.getData());
            }
        });
        this.patternY.setMergePreviousEvents(event -> {
            for (Event e : this.patternX.getEvents()) {
                event.getData().putAll(e.getData());
            }
        });

        this.patternX.setAfterProcess(this::afterProcess);
        this.patternY.setAfterProcess(this::afterProcess);
    }


    private void afterProcess(Event event) {

        if (!this.isWaiting()) {
            this.getEvents().clear();
            this.getEvents().add(event);
        }
    }

    @Override
    public void init(WisdomApp wisdomApp) {

        this.patternX.init(wisdomApp);
        this.patternY.init(wisdomApp);
    }

    @Override
    public void setNextProcessor(Processor nextProcessor) {

        super.setNextProcessor(nextProcessor);
        this.patternX.setNextProcessor(nextProcessor);
        this.patternY.setNextProcessor(nextProcessor);
    }

    @Override
    public void process(Event event) {

        this.eventDistributor.process(event);
    }


    @Override
    public void setProcessConditionMet(Predicate<Event> processConditionMet) {

        this.patternX.setProcessConditionMet(processConditionMet);
        this.patternY.setProcessConditionMet(processConditionMet);
    }

    @Override
    public void setEmitConditionMet(Predicate<Event> emitConditionMet) {

        Predicate<Event> predicate = this.predicate.and(emitConditionMet);
        this.patternX.setEmitConditionMet(predicate);
        this.patternY.setEmitConditionMet(predicate);
    }

    @Override
    public boolean isWaiting() {

        if (type == Type.AND) {
            return this.patternX.isWaiting() || this.patternY.isWaiting();
        } else {
            return this.patternX.isWaiting() && this.patternY.isWaiting();
        }
    }

    @Override
    public void setMergePreviousEvents(Consumer<Event> mergePreviousEvents) {

        super.setMergePreviousEvents(mergePreviousEvents);
        this.patternX.setMergePreviousEvents(this.patternX.getMergePreviousEvents().andThen(mergePreviousEvents));
        this.patternY.setMergePreviousEvents(this.patternY.getMergePreviousEvents().andThen(mergePreviousEvents));
    }
}
