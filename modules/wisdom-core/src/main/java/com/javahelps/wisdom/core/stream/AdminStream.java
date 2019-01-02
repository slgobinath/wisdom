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

package com.javahelps.wisdom.core.stream;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.event.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Consumer;

public class AdminStream extends Stream {

    private static final Logger LOGGER = LoggerFactory.getLogger(AdminStream.class);

    private final Map<String, Set<Consumer<Map<String, Object>>>> consumers = new HashMap<>();

    public AdminStream(WisdomApp wisdomApp, String id) {
        super(wisdomApp, id);
    }

    public AdminStream(WisdomApp wisdomApp, String id, Properties properties) {
        super(wisdomApp, id, properties);
    }

    @Override
    public void process(Event event) {
        super.process(event);
        this.execute(event);
    }

    @Override
    public void process(List<Event> events) {
        super.process(events);
        events.forEach(this::execute);
    }

    private void execute(Event event) {
        String command = (String) event.get("command");
        if (command != null) {
            // Command is case sensitive
            Set<Consumer<Map<String, Object>>> set = this.consumers.get(command);
            if (set != null) {
                Map<String, Object> data = Collections.unmodifiableMap(event.getData());
                for (Consumer<Map<String, Object>> consumer : set) {
                    try {
                        consumer.accept(data);
                    } catch (Exception ex) {
                        LOGGER.error("Error in consumer", ex);
                    }
                }
            }
        }
    }

    public void register(String command, Consumer<Map<String, Object>> consumer) {
        Objects.requireNonNull(command, "Command cannot be null");
        Objects.requireNonNull(consumer, "Consumer cannot be null");
        Set<Consumer<Map<String, Object>>> set = this.consumers.get(command);
        if (set == null) {
            set = new LinkedHashSet<>();
            this.consumers.put(command, set);
        }
        set.add(consumer);
    }

    public void unregister(String command, Consumer<Map<String, Object>> consumer) {
        Objects.requireNonNull(command, "Command cannot be null");
        Objects.requireNonNull(consumer, "Consumer cannot be null");
        Set<Consumer<Map<String, Object>>> set = this.consumers.get(command);
        if (set != null) {
            set.remove(consumer);
        }
    }
}
