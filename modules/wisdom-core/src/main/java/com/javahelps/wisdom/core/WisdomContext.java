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

package com.javahelps.wisdom.core;

import com.javahelps.wisdom.core.time.*;
import com.javahelps.wisdom.core.util.WisdomConfig;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

import static com.javahelps.wisdom.core.util.WisdomConstants.ASYNC;
import static com.javahelps.wisdom.core.util.WisdomConstants.PLAYBACK;

/**
 * The user defined and system defined configurations of the stream processor are stored in {@link WisdomContext}.
 */
public class WisdomContext {

    private final boolean async;
    private final boolean playbackEnabled;
    private final String playbackAttribute;
    private final Scheduler scheduler;
    private final Properties properties;
    private final ExecutorService executorService;
    private final ThreadFactory threadFactory;
    private final ThreadBarrier threadBarrier;
    private final TimestampGenerator timestampGenerator;
    private final ScheduledExecutorService scheduledExecutorService;

    public WisdomContext(Properties properties) {
        this.properties = properties;
        this.scheduledExecutorService = Executors.newScheduledThreadPool(4);
        this.executorService = Executors.newCachedThreadPool();
        this.threadFactory = Executors.defaultThreadFactory();
        this.threadBarrier = new ThreadBarrier();
        this.async = (boolean) properties.getOrDefault(ASYNC, WisdomConfig.ASYNC_ENABLED);
        this.playbackAttribute = properties.getProperty(PLAYBACK);
        this.playbackEnabled = this.playbackAttribute != null;
        if (this.playbackEnabled) {
            this.timestampGenerator = new EventBasedTimestampGenerator();
            this.scheduler = new EventBasedScheduler(this.executorService, (EventBasedTimestampGenerator) this.timestampGenerator);
        } else {
            this.timestampGenerator = new SystemTimestampGenerator();
            this.scheduler = new SystemScheduler(this.scheduledExecutorService, this.timestampGenerator);
        }
    }

    public TimestampGenerator getTimestampGenerator() {
        return timestampGenerator;
    }

    public ScheduledExecutorService getScheduledExecutorService() {
        return scheduledExecutorService;
    }

    public Scheduler getScheduler() {
        return scheduler;
    }

    public ExecutorService getExecutorService() {
        return executorService;
    }

    public ThreadFactory getThreadFactory() {
        return threadFactory;
    }

    public ThreadBarrier getThreadBarrier() {
        return threadBarrier;
    }

    public void start() {

    }

    public void shutdown() {
        this.scheduledExecutorService.shutdown();
        this.executorService.shutdown();
    }

    public boolean isAsync() {
        return async;
    }

    public boolean isPlaybackEnabled() {
        return playbackEnabled;
    }

    public String getPlaybackAttribute() {
        return playbackAttribute;
    }

    public Comparable getProperty(Comparable property) {
        return (Comparable) this.properties.get(property);
    }
}
