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

import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class SystemScheduler implements Scheduler {

    private final ScheduledExecutorService scheduledExecutorService;
    private final TimestampGenerator timestampGenerator;

    public SystemScheduler(ScheduledExecutorService scheduledExecutorService, TimestampGenerator timestampGenerator) {
        this.scheduledExecutorService = scheduledExecutorService;
        this.timestampGenerator = timestampGenerator;
    }

    @Override
    public void schedule(Duration duration, Executor executor) {
        this.scheduledExecutorService.schedule(new TaskRunner(executor), duration.toMillis(), TimeUnit.MILLISECONDS);
    }

    private class TaskRunner implements Runnable {

        private final Executor executor;

        private TaskRunner(Executor executor) {
            this.executor = executor;
        }

        @Override
        public void run() {
            try {
                this.executor.execute(timestampGenerator.currentTimestamp());
            } catch (Exception ex) {

            }
        }
    }
}
