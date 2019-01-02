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

package com.javahelps.wisdom.core.event;

import com.javahelps.wisdom.core.operator.Operator;
import com.javahelps.wisdom.core.processor.AttributeSelectProcessor;

import java.util.function.Function;
import java.util.function.Supplier;

/**
 * {@link Attribute} provides some built-in operations on the given attribute of an {@link Event} at the runtime.
 * {@link Attribute} modifies the attributes of the {@link Event} which is passed attrName the parameter of the
 * {@link Function}.
 *
 * @see AttributeSelectProcessor
 */
public class Attribute extends Operator implements Supplier<Object> {

    private final String name;
    private final Supplier<Event> eventSupplier;

    public Attribute(Event event, String name) {
        this(() -> event, name);
    }

    public Attribute(Supplier<Event> eventSupplier, String name) {
        this.name = name;
        this.eventSupplier = eventSupplier;
    }

    public static Function<Event, Object> of(String name) {
        return event -> event.get(name);
    }

    @Override
    public Object get() {
        return this.eventSupplier.get().get(this.name);
    }
}
