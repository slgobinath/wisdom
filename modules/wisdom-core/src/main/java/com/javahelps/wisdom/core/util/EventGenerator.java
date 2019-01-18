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

package com.javahelps.wisdom.core.util;

import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.exception.EventValidationException;

import java.util.Map;
import java.util.Objects;

/**
 * A utility class to create {@link Event}s from different type of inputs.
 */
public class EventGenerator {

    private static final Event RESET_EVENT = new Event(-1L);

    static {
        RESET_EVENT.setReset(true);
    }

    private EventGenerator() {
    }

    public static Event generate(Comparable... entries) {
        int count = entries.length;
        if (count % 2 != 0) {
            throw new EventValidationException("The given values must be key, value pairs with even number of " +
                    "parameters");
        } else {
            Event event = new Event(System.currentTimeMillis());
            for (int i = 0; i < count; i += 2) {
                String key = Objects.toString(entries[i]);
                Object value = entries[i + 1];
                if (value instanceof Integer) {
                    value = ((Integer) value).longValue();
                } else if (value instanceof Float) {
                    value = ((Float) value).doubleValue();
                }
                event.set(key, value);
            }
            return event;
        }
    }

    public static Event generate(Map<String, Object> map) {
        Event event = new Event(System.currentTimeMillis());
        event.getData().putAll(map);
        return event;
    }

    public static Event getResetEvent() {
        return RESET_EVENT;
    }
}
