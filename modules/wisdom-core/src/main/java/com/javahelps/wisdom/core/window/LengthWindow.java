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
 * Window keeps n number of events.
 */
@WisdomExtension("length")
public class LengthWindow extends Window implements Variable.OnUpdateListener<Number> {

    private List<Event> events;
    private int length;
    private Variable<Number> lengthVariable;

    public LengthWindow(Map<String, ?> properties) {
        super(properties);
        Object val = this.getProperty("length", 0);
        if (val instanceof Variable) {
            this.lengthVariable = (Variable<Number>) val;
            this.length = this.lengthVariable.get().intValue();
            this.lengthVariable.addOnUpdateListener(this);
        } else if (val instanceof Number) {
            this.length = ((Number) val).intValue();
        } else {
            throw new WisdomAppValidationException("length of LengthWindow must be java.lang.Integer but found %s", val.getClass().getCanonicalName());
        }
        this.events = new ArrayList<>(this.length);
    }

    public void process(Event event, Processor nextProcessor) {
        events.add(event);
        List<Event> eventsToSend = null;
        try {
            this.lock.lock();
            if (events.size() >= length) {
                eventsToSend = new ArrayList<>(events);
                events.clear();
            }
        } finally {
            this.lock.unlock();
        }

        if (eventsToSend != null) {
            nextProcessor.process(eventsToSend);
        }
    }

    @Override
    public Window copy() {

        return new LengthWindow(this.properties);
    }

    @Override
    public void clear() {
        try {
            this.lock.lock();
            this.events.clear();
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public void destroy() {
        if (this.lengthVariable != null) {
            this.lengthVariable.removeOnUpdateListener(this);
        }
        this.events = null;
    }

    @Override
    public void update(Number value) {
        try {
            this.lock.lock();
            this.length = value.intValue();
        } finally {
            this.lock.unlock();
        }
    }
}
