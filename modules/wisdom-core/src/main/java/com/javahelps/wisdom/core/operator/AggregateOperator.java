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

package com.javahelps.wisdom.core.operator;

import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.extension.ImportsManager;
import com.javahelps.wisdom.core.partition.Partitionable;
import com.javahelps.wisdom.core.processor.Stateful;

import java.util.Map;
import java.util.function.Function;

public abstract class AggregateOperator implements Function<Event, Object>, Stateful, Partitionable {

    protected final String newName;

    static {
        ImportsManager.INSTANCE.use(AggregateOperator.class.getPackageName());
    }

    protected AggregateOperator(String as, Map<String, ?> properties) {
        this.newName = as;
    }

    public String getNewName() {
        return newName;
    }

    public static AggregateOperator create(String namespace, String as, Map<String, ?> properties) {
        return ImportsManager.INSTANCE.createAggregateOperator(namespace, as, properties);
    }
}
