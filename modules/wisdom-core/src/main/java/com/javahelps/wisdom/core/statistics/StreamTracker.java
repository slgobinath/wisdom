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

package com.javahelps.wisdom.core.statistics;

import java.util.concurrent.atomic.AtomicLong;

public class StreamTracker {

    private final String streamId;
    private final AtomicLong count = new AtomicLong();
    private long startTime;

    public StreamTracker(String streamId) {
        this.streamId = streamId;
    }

    public void inEvent() {
        this.inEvent(1L);
    }

    public void inEvent(long count) {
        this.count.addAndGet(count);
    }

    void reset() {
        this.count.set(0L);
    }

    long getStartTime() {
        return startTime;
    }

    void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public String getStreamId() {
        return streamId;
    }

    public long getCount() {
        return this.count.get();
    }
}
