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
import com.javahelps.wisdom.core.exception.WisdomAppRuntimeException;

import java.util.Arrays;
import java.util.List;

public class EnsureProcessor extends StreamProcessor {

    private final int[] bounds;

    public EnsureProcessor(String id, int... bounds) {
        super(id);
        this.bounds = bounds;
        Arrays.sort(this.bounds);
    }

    @Override
    public void start() {

    }

    @Override
    public void process(Event event) {
        throw new WisdomAppRuntimeException("EnsureProcessor cannot be used with single event");
    }

    @Override
    public void process(List<Event> events) {
        int noOfEvents = events.size();
        for (int bound : this.bounds) {
            if (noOfEvents <= bound) {
                int newEvents = bound - noOfEvents;
                for (int i = 0; i < newEvents; i++) {
                    events.add(events.get(0).emptyEvent());
                }
                break;
            }
        }
        this.getNextProcessor().process(events);
    }

    @Override
    public Processor copy() {
        return this;
    }

    @Override
    public void destroy() {

    }
}
