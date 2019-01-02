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

package com.javahelps.wisdom.query;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.stream.StreamCallback;
import org.junit.Assert;
import org.slf4j.Logger;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by gobinath on 7/10/17.
 */
public class TestUtil {

    public static Map<String, Comparable> map(Comparable... entries) {
        int count = entries.length;
        Map<String, Comparable> map = new HashMap<>(count / 2);
        for (int i = 0; i < count; i += 2) {
            map.put((String) entries[i], entries[i + 1]);
        }
        return map;
    }

    public static TestCallback addStreamCallback(Logger logger, WisdomApp wisdomApp, String query, Map<String,
            Comparable>... expectedEvents) {
        TestCallback testCallback = new TestCallback(logger);
        testCallback.addCallback(wisdomApp, query, expectedEvents);
        return testCallback;
    }


    public static class TestCallback {

        private final Logger logger;
        private final AtomicInteger eventCount = new AtomicInteger();

        public TestCallback(Logger logger) {
            this.logger = logger;
        }

        private void addCallback(WisdomApp wisdomApp, String stream, Map<String, Comparable>... expectedEvents) {

            wisdomApp.addCallback(stream, new StreamCallback() {

                private int currentIndex = 0;

                @Override
                public void receive(Event... arrivedEvents) {
                    logger.info(Arrays.toString(arrivedEvents));
                    eventCount.addAndGet(arrivedEvents.length);
                    if (expectedEvents.length > 0 && currentIndex < expectedEvents.length) {
                        for (Event event : arrivedEvents) {
                            if (expectedEvents.length > currentIndex) {
                                Assert.assertEquals("Incorrect event", expectedEvents[currentIndex++], event.getData());
                            }
                        }
                    }
                }
            });
        }

        public int getEventCount() {
            return this.eventCount.get();
        }
    }
}
