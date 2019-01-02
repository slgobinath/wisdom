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
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * {@link StreamProcessor} to modify or map into a new {@link Event} based on a {@link Function}.
 */
public class MapProcessor extends StreamProcessor {

    private Function<Event, Event> function;

    public MapProcessor(String id, Function<Event, Event> function) {
        super(id);
        this.function = function;
    }

    @Override
    public void start() {

    }

    @Override
    public void process(Event event) {
        this.getNextProcessor().process(this.function.apply(event));
    }

    @Override
    public void process(List<Event> events) {
        List<Event> eventsAfterMapping = events.stream().map(this.function).collect(Collectors.toList());
        this.getNextProcessor().process(eventsAfterMapping);
    }

    @Override
    public Processor copy() {

        MapProcessor mapProcessor = new MapProcessor(this.id, this.function);
        mapProcessor.setNextProcessor(this.getNextProcessor().copy());
        return mapProcessor;
    }

    @Override
    public void destroy() {
        this.function = null;
    }
}
