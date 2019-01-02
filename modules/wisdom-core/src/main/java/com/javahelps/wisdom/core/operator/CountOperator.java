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
import com.javahelps.wisdom.core.extension.WisdomExtension;
import com.javahelps.wisdom.core.partition.Partitionable;

import java.util.Collections;
import java.util.Map;

@WisdomExtension("count")
public class CountOperator extends AggregateOperator {

    private long count;

    public CountOperator(String as, Map<String, ?> properties) {
        super(as, properties);
    }

    @Override
    public Object apply(Event event) {
        long value;
        synchronized (this) {
            if (event.isReset()) {
                this.count = 0;
            } else {
                this.count++;
            }
            value = this.count;
        }
        return value;
    }

    @Override
    public void clear() {
        synchronized (this) {
            this.count = 0L;
        }
    }

    @Override
    public Partitionable copy() {
        return new CountOperator(this.newName, Collections.EMPTY_MAP);
    }

    @Override
    public void destroy() {

    }
}
