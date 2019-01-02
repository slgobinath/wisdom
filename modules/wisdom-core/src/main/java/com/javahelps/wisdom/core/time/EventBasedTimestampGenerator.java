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

package com.javahelps.wisdom.core.time;

import java.util.ArrayList;
import java.util.List;

/**
 * A {@link TimestampGenerator} which returns the system timestamp.
 */
public class EventBasedTimestampGenerator implements TimestampGenerator {

    private long currentTimestamp = 0;
    private List<TimeChangeListener> timeChangeListeners = new ArrayList<>();


    public void addTimeChangeListener(TimeChangeListener listener) {
        this.timeChangeListeners.add(listener);
    }

    public void removeTimeChangeListener(TimeChangeListener listener) {
        this.timeChangeListeners.remove(listener);
    }

    public void setCurrentTimestamp(long currentTimestamp) {
        synchronized (this) {
            if (this.currentTimestamp >= currentTimestamp) {
                return;
            }
            this.currentTimestamp = currentTimestamp;
        }
        for (TimeChangeListener listener : this.timeChangeListeners) {
            listener.onTimeChange(currentTimestamp);
        }
    }

    public synchronized long currentTimestamp() {
        return this.currentTimestamp;
    }

    public interface TimeChangeListener {
        void onTimeChange(long timestamp);
    }
}
