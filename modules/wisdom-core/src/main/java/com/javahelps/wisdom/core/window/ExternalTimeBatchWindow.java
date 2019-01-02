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

package com.javahelps.wisdom.core.window;

import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.exception.WisdomAppValidationException;
import com.javahelps.wisdom.core.extension.WisdomExtension;
import com.javahelps.wisdom.core.processor.Processor;
import com.javahelps.wisdom.core.variable.Variable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * TimeBatchWindow depending on external timestamp.
 */
@WisdomExtension("externalTimeBatch")
public class ExternalTimeBatchWindow extends Window implements Variable.OnUpdateListener<Number> {

    private final String timestampKey;
    private long timeToKeep;
    private List<Event> events = new ArrayList<>();
    private long endTime = -1;
    private Variable<Number> timeVariable;

    public ExternalTimeBatchWindow(Map<String, ?> properties) {
        super(properties);
        Object keyVal = this.getProperty("timestampKey", 0);
        Object durationVal = this.getProperty("duration", 1);
        if (keyVal instanceof String) {
            this.timestampKey = (String) keyVal;
        } else {
            throw new WisdomAppValidationException("timestampKey of ExternalTimeBatchWindow must be java.lang.String but found %d", keyVal.getClass().getSimpleName());
        }
        if (durationVal instanceof Number) {
            this.timeToKeep = ((Number) durationVal).longValue();
        } else if (durationVal instanceof Variable) {
            this.timeVariable = (Variable<Number>) durationVal;
            this.timeToKeep = this.timeVariable.get().longValue();
            this.timeVariable.addOnUpdateListener(this);
        } else {
            throw new WisdomAppValidationException("duration of ExternalTimeBatchWindow must be long but found %d",
                    durationVal.getClass().getSimpleName());
        }
    }

    @Override
    public void process(Event event, Processor nextProcessor) {

        List<Event> eventsToSend = null;
        long currentTimestamp = event.getAsLong(this.timestampKey);

        try {
            this.lock.lock();
            if (events.isEmpty()) {
                this.endTime = currentTimestamp + this.timeToKeep;
            }

            if (currentTimestamp >= this.endTime) {
                // Timeout happened
                eventsToSend = new ArrayList<>(this.events);
                this.events.clear();
                this.endTime = this.findEndTime(currentTimestamp, this.endTime, this.timeToKeep);
            }
            this.events.add(event);
        } finally {
            this.lock.unlock();
        }

        if (eventsToSend != null) {
            nextProcessor.process(eventsToSend);
        }
    }

    private long findEndTime(long currentTime, long preEndTime, long timeToKeep) {
        // returns the next emission time based on system clock round time values.
        long elapsedTimeSinceLastEmit = (currentTime - preEndTime) % timeToKeep;
        return (currentTime + (timeToKeep - elapsedTimeSinceLastEmit));
    }

    @Override
    public Window copy() {
        return new ExternalTimeBatchWindow(this.properties);
    }

    @Override
    public void clear() {
        try {
            this.lock.lock();
            this.events.clear();
            this.endTime = -1;
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public void destroy() {
        if (this.timeVariable != null) {
            this.timeVariable.removeOnUpdateListener(this);
        }
        this.events = null;
    }

    @Override
    public void update(Number value) {
        try {
            this.lock.lock();
            long val = value.longValue();
            this.endTime = this.endTime - this.timeToKeep + val;
            this.timeToKeep = val;
        } finally {
            this.lock.unlock();
        }
    }
}
