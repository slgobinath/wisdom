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

package com.javahelps.wisdom.core.stream.output;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.extension.ImportsManager;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public abstract class Sink {

    static {
        ImportsManager.INSTANCE.use(ConsoleSink.class);
    }

    public Sink(Map<String, ?> properties) {

    }

    public static Sink create(String namespace, Map<String, ?> properties) {
        return ImportsManager.INSTANCE.createSink(namespace, properties);
    }

    public abstract void start();

    public abstract void init(WisdomApp wisdomApp, String streamId);

    public abstract void publish(List<Event> events) throws IOException;

    public abstract void stop();
}
