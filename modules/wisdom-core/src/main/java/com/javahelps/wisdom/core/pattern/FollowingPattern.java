package com.javahelps.wisdom.core.pattern;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.processor.Processor;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Created by gobinath on 6/29/17.
 */
class FollowingPattern extends CustomPattern {


    private Pattern first;
    private Pattern next;

    FollowingPattern(String patternId, Pattern first, Pattern next) {
        super(patternId);
        this.first = first;
        this.next = next;
        this.eventDistributor.add(first);
        this.eventDistributor.add(next);

        this.first.setProcessConditionMet(event -> true);
        this.next.setProcessConditionMet(event -> this.first.isComplete());

        this.first.setEmitConditionMet(event -> false);
        this.next.setEmitConditionMet(event -> true);

        this.next.setPreviousEvents(this.first::getEvents);

        this.first.setPreProcess(event -> this.next.onPreviousPreProcess(event));
        this.first.setPostProcess(event -> this.next.onPreviousPostProcess(event));

        this.next.setPreProcess(event -> this.first.onNextPreProcess(event));
        this.next.setPostProcess(event -> this.first.onNextPostProcess(event));

        if (this.first.isBatchPattern()) {
            this.next.setBatchPattern(true);
        }
        if (this.next.isBatchPattern()) {
            this.setBatchPattern(true);
        }

        // Add th streams to this pattern
        this.streamIds.addAll(this.first.streamIds);
        this.streamIds.addAll(this.next.streamIds);
    }

    @Override
    public void onNextPreProcess(Event event) {
        this.next.onNextPreProcess(event);
    }

    @Override
    public void onNextPostProcess(Event event) {
        this.reset();
    }

    @Override
    public void onPreviousPreProcess(Event event) {
        this.first.onPreviousPreProcess(event);
    }

    @Override
    public void onPreviousPostProcess(Event event) {
        this.first.onPreviousPostProcess(event);
    }

    @Override
    public void reset() {
        this.first.reset();
        this.next.reset();
    }

    @Override
    public void init(WisdomApp wisdomApp) {

        this.first.init(wisdomApp);
        this.next.init(wisdomApp);
        this.first.streamIds.forEach(streamId -> {
            wisdomApp.getStream(streamId).removeProcessor(this.first);
            wisdomApp.getStream(streamId).addProcessor(this);
        });
        this.next.streamIds.forEach(streamId -> {
            wisdomApp.getStream(streamId).removeProcessor(this.next);
            wisdomApp.getStream(streamId).addProcessor(this);
        });
    }

    @Override
    public Event event() {

        return next.event();
    }

    @Override
    public boolean isAccepting() {
        return first.isAccepting() || next.isAccepting();
    }

    @Override
    public void setAccepting(boolean accepting) {
        if (accepting) {
            if (first.isAccepting()) {
                next.setAccepting(true);
                return;
            } else {
                // First already accepted
                if (next.isAccepting()) {
                    return;
                } else {
                    // Second also already accepted
                    next.setAccepting(true);
                }
            }
        } else {
            first.setAccepting(false);
            next.setAccepting(false);
        }
    }

    @Override
    public void setProcessConditionMet(Predicate<Event> processConditionMet) {
        this.first.setProcessConditionMet(processConditionMet);
    }

    @Override
    public void setEmitConditionMet(Predicate<Event> emitConditionMet) {
        this.next.setEmitConditionMet(emitConditionMet);
    }

    @Override
    public void setNextProcessor(Processor nextProcessor) {
        super.setNextProcessor(nextProcessor);
        this.first.setNextProcessor(nextProcessor);
        this.next.setNextProcessor(nextProcessor);
    }

    @Override
    public void process(Event event) {
        this.first.setConsumed(false);
        this.next.setConsumed(false);
        this.eventDistributor.process(event);
    }

    @Override
    public boolean isConsumed() {
        return this.first.isConsumed() || this.next.isConsumed();
    }

    @Override
    public void setConsumed(boolean consumed) {
        this.first.setConsumed(false);
        this.next.setConsumed(false);
    }

    @Override
    public boolean isComplete() {
        return this.next.isComplete();
    }

    @Override
    public void setPreviousEvents(Supplier<List<Event>> previousEvents) {
        this.first.setPreviousEvents(previousEvents);
    }

    @Override
    public void setBatchPattern(boolean batchPattern) {
        super.setBatchPattern(batchPattern);
        this.first.setBatchPattern(batchPattern);
        this.next.setBatchPattern(batchPattern);
    }

    @Override
    public List<Event> getEvents() {

        List<Event> events = new ArrayList<>();
        if (this.next instanceof EmptiablePattern) {
            events.addAll(((EmptiablePattern) this.next).getEvents(false));
        } else {
            events.addAll(this.next.getEvents());
        }

        return events;
    }

    @Override
    public void setPostProcess(Consumer<Event> postProcess) {
        this.next.setPostProcess(postProcess);
    }

    public void setWithin(long timestamp) {
        this.next.setExpiredCondition((currentEvent, preEvent) -> currentEvent.getTimestamp() - preEvent.getTimestamp
                () > timestamp);
    }

    @Override
    public void setExpiredCondition(BiFunction<Event, Event, Boolean> expiredCondition) {
        this.next.setExpiredCondition(expiredCondition);
    }
}
