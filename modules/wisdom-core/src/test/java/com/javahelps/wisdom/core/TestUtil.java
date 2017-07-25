package com.javahelps.wisdom.core;

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


    public static class CallbackUtil {

        private final Logger logger;
        private final AtomicInteger eventCount;

        public CallbackUtil(Logger logger, AtomicInteger eventCount) {
            this.logger = logger;
            this.eventCount = eventCount;
        }

        public void addCallback(WisdomApp wisdomApp, Map<String, Comparable>... expectedEvents) {

            wisdomApp.addCallback("OutputStream", arrivedEvents -> {
                logger.info(Arrays.toString(arrivedEvents));
                int count = eventCount.addAndGet(arrivedEvents.length);
                if (expectedEvents.length > 0) {
                    Assert.assertEquals("Incorrect event", expectedEvents[count - 1], arrivedEvents[0].getData());
                }

            });
        }
    }
}
