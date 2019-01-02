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

import java.util.List;
import java.util.function.Predicate;

/**
 * {@link StreamProcessor} to filter the events based on a given {@link Predicate}.
 */
public class FilterProcessor extends StreamProcessor {

    private Predicate<Event> predicate;

    public FilterProcessor(String id, Predicate<Event> predicate) {
        super(id);
        this.predicate = predicate;
    }

    @Override
    public void start() {

    }

    @Override
    public void process(Event event) {
        if (this.predicate.test(event)) {
            this.getNextProcessor().process(event);
        }
    }

    @Override
    public void process(List<Event> events) {
        events.removeIf(this.predicate.negate());
        if (!events.isEmpty()) {
            this.getNextProcessor().process(events);
        }
    }

    @Override
    public Processor copy() {

        FilterProcessor filterProcessor = new FilterProcessor(this.id, this.predicate);
        filterProcessor.setNextProcessor(this.getNextProcessor().copy());
        return filterProcessor;
    }

    @Override
    public void destroy() {
        this.predicate = null;
    }
}
