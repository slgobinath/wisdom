package com.javahelps.wisdom.core.pattern;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.processor.Processor;

import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Created by gobinath on 6/29/17.
 */
public class LogicalPattern extends Pattern {

    public enum Type {
        OR, AND;
    }


    private Type type;
    private Pattern first;
    private Pattern next;

    public LogicalPattern(String patternId, Type type, Pattern first, Pattern next) {
        super(patternId);
        this.type = type;

        this.first = first;
        this.next = next;
        this.eventDistributor.add(first);
        this.eventDistributor.add(next);

        this.first.setProcessConditionMet(event -> true);
        this.next.setProcessConditionMet(event -> true);

        this.first.setEvents(this.getEvents());
        this.first.setEvents(this.getEvents());

        Predicate<Event> predicate;
        if (type == Type.AND) {
            predicate = event -> !this.first.isWaiting() && !this.next.isWaiting();
        } else {
            // OR
            predicate = event -> !this.first.isWaiting() || !this.next.isWaiting();
        }
        this.predicate = predicate;

        this.first.setMergePreviousEvents(event -> {
            for (Event e : this.next.getEvents()) {
                event.getData().putAll(e.getData());
            }
        });
        this.next.setMergePreviousEvents(event -> {
            for (Event e : this.first.getEvents()) {
                event.getData().putAll(e.getData());
            }
        });

        this.first.setAfterProcess(this::afterProcess);
        this.next.setAfterProcess(this::afterProcess);
    }


    private void afterProcess(Event event) {
        if (!this.isWaiting()) {
            this.getEvents().clear();
            this.getEvents().add(event);
        }
    }

    @Override
    public void init(WisdomApp wisdomApp) {

        this.first.init(wisdomApp);
        this.next.init(wisdomApp);
    }

    @Override
    public void setNextProcessor(Processor nextProcessor) {
        super.setNextProcessor(nextProcessor);
        this.first.setNextProcessor(nextProcessor);
        this.next.setNextProcessor(nextProcessor);
    }

    @Override
    public void process(Event event) {
        this.eventDistributor.process(event);
    }


    @Override
    public void setProcessConditionMet(Predicate<Event> processConditionMet) {
        this.first.setProcessConditionMet(processConditionMet);
        this.next.setProcessConditionMet(processConditionMet);
    }

    @Override
    public void setEmitConditionMet(Predicate<Event> emitConditionMet) {
        Predicate<Event> predicate = this.predicate.and(emitConditionMet);
        this.first.setEmitConditionMet(predicate);
        this.next.setEmitConditionMet(predicate);
    }

    @Override
    public boolean isWaiting() {
        if (type == Type.AND) {
            return this.first.isWaiting() || this.next.isWaiting();
        } else {
            return this.first.isWaiting() && this.next.isWaiting();
        }
    }

    @Override
    public void setMergePreviousEvents(Consumer<Event> mergePreviousEvents) {
        this.first.setMergePreviousEvents(this.first.getMergePreviousEvents().andThen(mergePreviousEvents));
        this.next.setMergePreviousEvents(this.next.getMergePreviousEvents().andThen(mergePreviousEvents));
    }
}
