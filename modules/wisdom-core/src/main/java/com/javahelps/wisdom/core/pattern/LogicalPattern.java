/*
 * Copyright (c) 2018, Gobinath Loganathan (http://github.com/slgobinath) All Rights Reserved.
 *
 * Gobinath licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. In addition, if you are using
 * this file in your research work, you are required to cite
 * WISDOM as mentioned at https://github.com/slgobinath/wisdom.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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

    //    @Override
//    public void setMergePreviousEvents(Consumer<Event> mergePreviousEvents) {
//
//        super.setMergePreviousEvents(mergePreviousEvents);
//        this.patternX.setMergePreviousEvents(this.patternX.getMergePreviousEvents().andThen(mergePreviousEvents));
//        this.patternY.setMergePreviousEvents(this.patternY.getMergePreviousEvents().andThen(mergePreviousEvents));
//    }

    public enum Type {
        OR, AND
    }
}
