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

import java.util.Arrays;
import java.util.List;

/**
 * {@link StreamProcessor} to create a new {@link Event} with a subset of the attributes of previous {@link Event}.
 */
public class AttributeSelectProcessor extends StreamProcessor {

    private final boolean selectAll;
    private List<String> attributes;

    public AttributeSelectProcessor(String id, String... attributes) {
        super(id);
        this.selectAll = attributes.length == 0;
        this.attributes = Arrays.asList(attributes);
    }

    public void init() {

    }

    @Override
    public void start() {

    }


    @Override
    public void process(Event event) {
        if (!selectAll) {
            event.getData().keySet().retainAll(attributes);
        }
        this.getNextProcessor().process(event);
    }

    @Override
    public void process(List<Event> events) {
        if (!selectAll) {
            for (Event event : events) {
                event.getData().keySet().retainAll(attributes);
            }
        }
        this.getNextProcessor().process(events);
    }

    @Override
    public Processor copy() {

        AttributeSelectProcessor attributeSelectProcessor = new AttributeSelectProcessor(this.id, this.attributes.toArray(new String[0]));
        attributeSelectProcessor.setNextProcessor(this.getNextProcessor().copy());
        return attributeSelectProcessor;
    }

    @Override
    public void destroy() {
        this.attributes = null;
    }
}
