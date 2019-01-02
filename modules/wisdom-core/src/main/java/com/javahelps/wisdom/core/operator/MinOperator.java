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
import com.javahelps.wisdom.core.exception.WisdomAppValidationException;
import com.javahelps.wisdom.core.extension.WisdomExtension;
import com.javahelps.wisdom.core.partition.Partitionable;
import com.javahelps.wisdom.core.util.Commons;

import java.util.Comparator;
import java.util.Map;

import static com.javahelps.wisdom.core.util.WisdomConstants.ATTR;

@WisdomExtension("min")
public class MinOperator extends AggregateOperator {

    private final Comparator<Comparable> naturalOrder = Comparator.naturalOrder();
    private final Comparator<Comparable> comparator = Comparator.nullsLast(naturalOrder);
    private String attribute;
    private Comparable min;

    public MinOperator(String as, Map<String, ?> properties) {
        super(as, properties);
        this.attribute = Commons.getProperty(properties, ATTR, 0);
        if (this.attribute == null) {
            throw new WisdomAppValidationException("Required property %s of Min operator not found", ATTR);
        }
    }

    @Override
    public Object apply(Event event) {
        Comparable value;
        synchronized (this) {
            if (event.isReset()) {
                value = null;
            } else {
                Comparable newReference = (Comparable) event.get(attribute);
                if (comparator.compare(newReference, this.min) < 0) {
                    this.min = newReference;
                }
                value = this.min;
            }
        }
        return value;
    }

    @Override
    public void clear() {
        synchronized (this) {
            this.min = null;
        }
    }

    @Override
    public Partitionable copy() {
        return new MinOperator(this.newName, Map.of(ATTR, this.attribute));
    }

    @Override
    public void destroy() {
        this.min = null;
    }
}
