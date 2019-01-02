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

package com.javahelps.wisdom.core.processor;

import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.event.Index;
import com.javahelps.wisdom.core.exception.WisdomAppRuntimeException;

import java.util.ArrayList;
import java.util.List;

/**
 * {@link StreamProcessor} to create a new {@link Event} with a subset of the attributes of previous {@link Event}.
 */
public class EventSelectProcessor extends StreamProcessor {

    private Index index;

    public EventSelectProcessor(String id, Index index) {
        super(id);
        this.index = index;
    }

    public void init() {

    }

    @Override
    public void start() {

    }


    @Override
    public void process(Event event) {
        throw new WisdomAppRuntimeException("EventSelector cannot be used with single event stream.");
    }

    @Override
    public void process(List<Event> events) {
        int length = events.size();
        if (length > 0) {
            int[] indices = this.index.getIndices();
            List<Event> selectedEvents = new ArrayList<>(indices.length);
            for (int i : indices) {
                if (i >= 0) {
                    selectedEvents.add(events.get(i));
                } else {
                    selectedEvents.add(events.get(length + i));
                }
            }
            this.getNextProcessor().process(selectedEvents);
        }
    }

    @Override
    public Processor copy() {

        EventSelectProcessor eventSelectProcessor = new EventSelectProcessor(this.id, this.index);
        eventSelectProcessor.setNextProcessor(this.getNextProcessor().copy());
        return eventSelectProcessor;
    }

    @Override
    public void destroy() {

    }
}
