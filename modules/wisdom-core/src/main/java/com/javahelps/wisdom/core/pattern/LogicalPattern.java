package com.javahelps.wisdom.core.pattern;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.processor.Processor;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

/**
 * Created by gobinath on 6/29/17.
 */
class LogicalPattern extends CustomPattern {

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
            predicate = event -> this.patternX.isComplete() && this.patternY.isComplete();
        } else {
            // OR
            predicate = event -> this.patternX.isComplete() || this.patternY.isComplete();
        }
        this.predicate = predicate;

//        this.patternX.setMergePreviousEvents(event -> {
//            for (Event e : this.patternY.getEvents()) {
//                event.getData().putAll(e.getData());
//            }
//        });
//        this.patternY.setMergePreviousEvents(event -> {
//            for (Event e : this.patternX.getEvents()) {
//                event.getData().putAll(e.getData());
//            }
//        });

        this.patternX.setPreviousEvents(() -> {
            List<Event> events = new ArrayList<>();
            events.addAll(this.getPreviousEvents().get());
            events.addAll(this.patternY.getEvents());
            return events;
        });
        this.patternY.setPreviousEvents(() -> {
            List<Event> events = new ArrayList<>();
            events.addAll(this.getPreviousEvents().get());
            events.addAll(this.patternX.getEvents());
            return events;
        });

        this.patternX.setPostProcess(this::afterProcess);
        this.patternY.setPostProcess(this::afterProcess);

        // Add th streams to this pattern
        this.streamIds.addAll(this.patternX.streamIds);
        this.streamIds.addAll(this.patternY.streamIds);
    }

    private void afterProcess(Event event) {

        if (this.isComplete()) {
            this.getEvents().clear();
            this.getEvents().add(event);
        }
    }

    @Override
    public void init(WisdomApp wisdomApp) {

//        this.patternX.init(wisdomApp);
//        this.patternY.init(wisdomApp);
        this.patternX.init(wisdomApp);
        this.patternY.init(wisdomApp);
        this.patternX.streamIds.forEach(streamId -> {
            wisdomApp.getStream(streamId).removeProcessor(this.patternX);
        });
        this.patternY.streamIds.forEach(streamId -> {
            wisdomApp.getStream(streamId).removeProcessor(this.patternY);
        });
        super.init(wisdomApp);
    }

    @Override
    public void setNextProcessor(Processor nextProcessor) {

        super.setNextProcessor(nextProcessor);
        this.patternX.setNextProcessor(nextProcessor);
        this.patternY.setNextProcessor(nextProcessor);
    }

    @Override
    public void process(Event event) {

        try {
            this.lock.lock();
            this.patternX.setConsumed(false);
            this.patternY.setConsumed(false);
            this.eventDistributor.process(event);
        } finally {
            this.lock.unlock();
        }
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
    public boolean isComplete() {

        if (type == Type.AND) {
            return this.patternX.isComplete() && this.patternY.isComplete();
        } else {
            return this.patternX.isComplete() || this.patternY.isComplete();
        }
    }

    @Override
    public boolean isConsumed() {
        return this.patternX.isConsumed() || this.patternY.isConsumed();
    }

    @Override
    public void setConsumed(boolean consumed) {
        this.patternX.setConsumed(false);
        this.patternY.setConsumed(false);
    }

    public enum Type {
        OR, AND
    }

    //    @Override
//    public void setMergePreviousEvents(Consumer<Event> mergePreviousEvents) {
//
//        super.setMergePreviousEvents(mergePreviousEvents);
//        this.patternX.setMergePreviousEvents(this.patternX.getMergePreviousEvents().andThen(mergePreviousEvents));
//        this.patternY.setMergePreviousEvents(this.patternY.getMergePreviousEvents().andThen(mergePreviousEvents));
//    }

    @Override
    public void clear() {
        try {
            this.lock.lock();
            this.patternX.clear();
            this.patternY.clear();
        } finally {
            this.lock.unlock();
        }
    }
}
