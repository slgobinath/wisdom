package com.javahelps.wisdom.dev.test;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.stream.StreamCallback;
import org.slf4j.Logger;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class TestCallback {

    private final Logger logger;


    public TestCallback(Logger logger) {
        this.logger = logger;
    }

    public TestResult addCallback(WisdomApp wisdomApp, String streamId, Map<String, Object>... expectedEvents) {

        final TestResult result = new TestResult();

        wisdomApp.addCallback(streamId, new StreamCallback() {

            private int currentIndex = 0;

            @Override
            public void receive(Event... arrivedEvents) {
                logger.info(Arrays.toString(arrivedEvents));
                result.addToCount(arrivedEvents.length);
                if (expectedEvents.length > 0 && currentIndex < expectedEvents.length) {
                    for (Event event : arrivedEvents) {
                        if (expectedEvents.length > currentIndex) {
                            if (!Objects.deepEquals(expectedEvents[currentIndex++], event.getData())) {
                                AssertionError error = new AssertionError(String.format("Incorrect event expected:<%s> but was:<%s>", expectedEvents[currentIndex++], event.getData()));
                                result.addError(error);
                                throw error;
                            }
                        }
                    }
                }
            }
        });
        return result;
    }

    public static class TestResult {
        private final AtomicInteger eventCount = new AtomicInteger();
        private final List<AssertionError> errors = new LinkedList<>();

        private void addToCount(int delta) {
            this.eventCount.addAndGet(delta);
        }

        private void addError(AssertionError error) {
            this.errors.add(error);
        }

        public int getEventCount() {
            return eventCount.get();
        }

        public void throwErrors() {
            for (AssertionError error : this.errors) {
                throw error;
            }
        }

        public void assertEventCount(int expectedCount) {
            int actualCount = this.getEventCount();
            if (expectedCount != actualCount) {
                throw new AssertionError(String.format("Incorrect number of events expected:<%s> but was:<%s>", expectedCount, actualCount));
            }
        }

        public void assertTestResult(int expectedCount) {
            this.throwErrors();
            this.assertEventCount(expectedCount);
        }
    }
}
