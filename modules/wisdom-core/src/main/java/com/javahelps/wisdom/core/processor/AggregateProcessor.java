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
import com.javahelps.wisdom.core.operator.AggregateOperator;

import java.util.List;
import java.util.function.Function;

/**
 * {@link StreamProcessor} to modify or map into a new {@link Event} based on a {@link Function}.
 */
public class AggregateProcessor extends StreamProcessor {

    private AggregateOperator[] operators;

    public AggregateProcessor(String id, AggregateOperator... operators) {
        super(id);
        this.operators = operators;
    }

    @Override
    public void start() {

    }

    @Override
    public void process(Event event) {
        for (AggregateOperator operator : this.operators) {
            event.set(operator.getNewName(), operator.apply(event));
            this.getNextProcessor().process(event);
        }
    }

    @Override
    public void process(List<Event> events) {
        int lastIndex = events.size() - 1;
        if (lastIndex >= 0) {
            for (int i = 0; i < lastIndex; i++) {
                for (AggregateOperator operator : this.operators) {
                    operator.apply(events.get(i));
                }
            }
            // Set the attribute only in last event
            Event lastEvent = events.get(lastIndex);

            for (AggregateOperator operator : this.operators) {
                lastEvent.set(operator.getNewName(), operator.apply(lastEvent));
                // Reset the operator
                operator.clear();
            }
            this.getNextProcessor().process(lastEvent);
        }
    }

    @Override
    public Processor copy() {

        AggregateProcessor mapProcessor = new AggregateProcessor(this.id, this.operators);
        mapProcessor.setNextProcessor(this.getNextProcessor().copy());
        return mapProcessor;
    }

    @Override
    public void destroy() {
        this.operators = null;
    }
}
