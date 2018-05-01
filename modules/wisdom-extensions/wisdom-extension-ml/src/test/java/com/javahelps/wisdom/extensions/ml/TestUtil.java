package com.javahelps.wisdom.extensions.ml;

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

    public static TestCallback addStreamCallback(Logger logger, WisdomApp wisdomApp, String stream, Map<String,
            Comparable>... expectedEvents) {
        TestCallback testCallback = new TestCallback(logger);
        testCallback.addCallback(wisdomApp, stream, expectedEvents);
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
