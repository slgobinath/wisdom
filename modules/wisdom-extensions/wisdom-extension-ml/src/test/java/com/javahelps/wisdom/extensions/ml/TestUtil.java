package com.javahelps.wisdom.extensions.ml;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.stream.StreamCallback;
import org.junit.Assert;
import org.slf4j.Logger;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by gobinath on 7/10/17.
 */
public class TestUtil {

    public static TestCallback addStreamCallback(Logger logger, WisdomApp wisdomApp, String stream, Map<String,
            Object>... expectedEvents) {
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

        private void addCallback(WisdomApp wisdomApp, String stream, Map<String, Object>... expectedEvents) {

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
