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
import com.javahelps.wisdom.core.time.Scheduler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Created by gobinath on 6/29/17.
 */
class NotPattern extends CustomPattern implements EmptiablePattern {

    private Pattern pattern;
    private Scheduler scheduler;
    private Event previousEvent;

    NotPattern(String patternId, Pattern pattern) {

        super(patternId);

        this.pattern = pattern;
        this.pattern.setProcessConditionMet(event -> true);
        this.streamIds.addAll(this.pattern.streamIds);
    }

    @Override
    public void onPreviousPostProcess(Event event) {

        try {
            this.lock.lock();
            if (duration != null) {
                this.previousEvent = event;
                scheduler.schedule(duration, this::timeoutHappend);
            }
        } finally {
            this.lock.unlock();
        }
    }

    public void timeoutHappend(long timestamp) {

        try {
            this.lock.lock();
            if (this.pattern.isAccepting()) {
                this.getNextProcessor().process(this.previousEvent);
            }
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public void init(WisdomApp wisdomApp) {

        this.pattern.init(wisdomApp);
        this.scheduler = wisdomApp.getContext().getScheduler();
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
        } finally {
            this.lock.unlock();
        }
    }


    @Override
    public void setProcessConditionMet(Predicate<Event> processConditionMet) {

        try {
            this.lock.lock();
            this.pattern.setProcessConditionMet(processConditionMet);
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public void setEmitConditionMet(Predicate<Event> emitConditionMet) {

        try {
            this.lock.lock();
            Predicate<Event> predicate = this.predicate.and(emitConditionMet);
            this.pattern.setEmitConditionMet(predicate);
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public boolean isAccepting() {

        try {
            this.lock.lock();
            return this.pattern.isAccepting();
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public boolean isConsumed() {

        try {
            this.lock.lock();
            return this.pattern.isConsumed();
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public void setConsumed(boolean consumed) {

        try {
            this.lock.lock();
            this.pattern.setConsumed(consumed);
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public boolean isComplete() {

        try {
            this.lock.lock();
            return !this.pattern.isComplete();
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public void setPreviousEvents(Supplier<List<Event>> previousEvents) {

        try {
            this.lock.lock();
            super.setPreviousEvents(previousEvents);
            this.pattern.setPreviousEvents(previousEvents);
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public List<Event> getEvents() {

        try {
            this.lock.lock();
            return this.getEvents(true);
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public List<Event> getEvents(boolean isFirst) {

        List<Event> events;
        try {
            this.lock.lock();
            if (this.pattern.getEvents().isEmpty()) {
                if (isFirst) {
                    events = new ArrayList<>();
                    events.add(EmptiablePattern.EMPTY_EVENT);
                } else {
                    events = this.getPreviousEvents().get();
                }
            } else {
                events = Collections.EMPTY_LIST;
            }
        } finally {
            this.lock.unlock();
        }
        return events;
    }

    @Override
    public void clear() {
        try {
            this.lock.lock();
            this.pattern.clear();
            this.previousEvent = null;
        } finally {
            this.lock.unlock();
        }
    }
}
