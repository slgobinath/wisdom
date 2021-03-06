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

package com.javahelps.wisdom.core.map;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.extension.ImportsManager;

import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.javahelps.wisdom.core.util.WisdomConstants.ATTR;
import static com.javahelps.wisdom.core.util.WisdomConstants.VALUE;

public abstract class Mapper implements Function<Event, Event> {

    static {
        ImportsManager.INSTANCE.use(Mapper.class.getPackageName());
    }

    protected final String attrName;
    private Predicate<Event> predicate = event -> true;

    public Mapper(String attrName, Map<String, ?> properties) {
        this.attrName = attrName;
    }

    public static Mapper create(String namespace, String newName, Map<String, ?> properties) {
        return ImportsManager.INSTANCE.createMapper(namespace, newName, properties);
    }

    public static Mapper FORMAT_TIME(String currentName, String newName) {
        return new FormatTimeMapper(newName, Map.of(ATTR, currentName));
    }

    public static Mapper LENGTH(String currentName, String newName) {
        return new LengthMapper(newName, Map.of(ATTR, currentName));
    }

    public static Mapper RENAME(String currentName, String newName) {
        return new RenameMapper(newName, Map.of(ATTR, currentName));
    }

    public static Mapper TO_INT(String currentName, String newName) {
        return new IntMapper(newName, Map.of(ATTR, currentName));
    }

    public static Mapper TO_LONG(String currentName, String newName) {
        return new LongMapper(newName, Map.of(ATTR, currentName));
    }

    public static Mapper TO_FLOAT(String currentName, String newName) {
        return new FloatMapper(newName, Map.of(ATTR, currentName));
    }

    public static Mapper TO_DOUBLE(String currentName, String newName) {
        return new DoubleMapper(newName, Map.of(ATTR, currentName));
    }

    public static Mapper CONSTANT(Object value, String newName) {
        return new ConstantMapper(newName, Map.of(VALUE, value));
    }

    public abstract void start();

    public abstract void init(WisdomApp wisdomApp);

    public abstract void stop();

    public abstract Event map(Event event);

    @Override
    public Event apply(Event event) {
        if (this.predicate.test(event)) {
            return this.map(event);
        } else {
            return event;
        }
    }

    public Mapper onlyIf(Predicate<Event> predicate) {
        this.predicate = predicate;
        return this;
    }
}
