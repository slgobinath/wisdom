package com.javahelps.wisdom.service.util;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.stream.StreamCallback;
import org.junit.Assert;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
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

    public static void execTestServer(long waitingTime) {
        String javaHome = System.getProperty("java.home");
        String javaBin = javaHome +
                File.separator + "bin" +
                File.separator + "java";
        String classpath = System.getProperty("java.class.path");
        String className = TestServer.class.getCanonicalName();

        ProcessBuilder builder = new ProcessBuilder(javaBin, "-cp", classpath, className, Long.toString(waitingTime));

        try {
            builder.start();
        } catch (IOException e) {
            throw new RuntimeException("Failed to execute the class in separate JVM", e);
        }
    }

    public static class CallbackUtil {

        private final Logger logger;
        private final AtomicInteger eventCount;

        public CallbackUtil(Logger logger, AtomicInteger eventCount) {
            this.logger = logger;
            this.eventCount = eventCount;
        }

        public void addCallback(WisdomApp wisdomApp, Map<String, Comparable>... expectedEvents) {

            wisdomApp.addCallback("OutputStream", new StreamCallback() {

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
    }
}
