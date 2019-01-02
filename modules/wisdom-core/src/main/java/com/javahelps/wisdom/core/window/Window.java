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

package com.javahelps.wisdom.core.window;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.exception.WisdomAppValidationException;
import com.javahelps.wisdom.core.extension.ImportsManager;
import com.javahelps.wisdom.core.partition.Partitionable;
import com.javahelps.wisdom.core.processor.Initializable;
import com.javahelps.wisdom.core.processor.Processor;
import com.javahelps.wisdom.core.processor.Stateful;
import com.javahelps.wisdom.core.util.Commons;
import com.javahelps.wisdom.core.variable.Variable;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A utility to construct Windows.
 */
public abstract class Window implements Partitionable, Stateful, Initializable {

    static {
        ImportsManager.INSTANCE.use(Window.class.getPackageName());
    }

    protected final Lock lock = new ReentrantLock();
    protected final Map<String, ?> properties;

    public Window(Map<String, ?> properties) {
        this.properties = properties;
    }

    public static Window create(String namespace, Map<String, ?> properties) {
        return ImportsManager.INSTANCE.createWindow(namespace, properties);
    }

    public static Window length(int length) {
        return new LengthWindow(Collections.singletonMap("length", length));
    }

    public static Window length(Variable<Integer> length) {
        return new LengthWindow(Collections.singletonMap("length", length));
    }

    public static Window lengthBatch(int length) {
        return new LengthBatchWindow(Collections.singletonMap("length", length));
    }

    public static Window lengthBatch(Variable<Integer> length) {
        return new LengthBatchWindow(Collections.singletonMap("length", length));
    }

    public static Window timeBatch(Duration duration) {
        return new TimeBatchWindow(Commons.map("duration", duration.toMillis()));
    }

    public static Window externalTimeBatch(String timestampKey, Duration duration) {
        return new ExternalTimeBatchWindow(Commons.map("timestampKey", timestampKey, "duration", duration.toMillis()));
    }

    public static Window externalIdleTimeBatch(String timestampKey, Duration duration) {
        return new ExternalIdleTimeBatchWindow(Commons.map("timestampKey", timestampKey, "duration", duration.toMillis()));
    }

    public static Window idleTimeLengthBatch(Duration duration, int length) {
        return new IdleTimeLengthBatchWindow(Commons.map("duration", duration.toMillis(), "length", length));
    }

    public abstract void process(Event event, Processor nextProcessor);

    public void process(List<Event> events, Processor nextProcessor) {
        for (Event event : events) {
            this.process(event, nextProcessor);
        }
    }

    @Override
    public abstract Window copy();

    public Object getProperty(String attribute, int index) {
        Object value = this.properties.get(attribute);
        if (value == null) {
            value = this.properties.get(String.format("_param_%d", index));
            if (value == null) {
                throw new WisdomAppValidationException("%s requires %s but not available", this.getClass().getSimpleName(), attribute);
            }
        }
        return value;
    }

    @Override
    public void init(WisdomApp app) {
        // Do nothing
    }
}
