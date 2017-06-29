package com.javahelps.wisdom.core.processor.selector;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.exception.WisdomAppRuntimeException;
import com.javahelps.wisdom.core.stream.StreamCallback;
import com.javahelps.wisdom.core.util.EventGenerator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by gobinath on 6/28/17.
 */
public class SelectorTestCase {

    private static final Logger LOGGER = LoggerFactory.getLogger(SelectorTestCase.class);
    private AtomicInteger eventCount;

    @Before
    public void init() {
        this.eventCount = new AtomicInteger(0);
    }

    @Test
    public void testSelector1() throws InterruptedException {
        LOGGER.info("Test selector 1 - OUT 2");

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("StockStream", "symbol", "price", "volume");
        wisdomApp.defineStream("OutputStream", "symbol", "price");

        wisdomApp.defineQuery("query1")
                .from("StockStream")
                .select("symbol", "price")
                .insertInto("OutputStream");

        this.addCallback(wisdomApp,
                map("symbol", "IBM", "price", 50.0),
                map("symbol", "WSO2", "price", 60.0));

        wisdomApp.send("StockStream", EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10));
        wisdomApp.send("StockStream", EventGenerator.generate("symbol", "WSO2", "price", 60.0, "volume", 15));

        Thread.sleep(100);

        Assert.assertEquals("Incorrect number of events", 2, eventCount.get());
    }

    @Test
    public void testSelector2() throws InterruptedException {
        LOGGER.info("Test selector 2 - OUT 2");

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("StockStream", "symbol", "price", "volume");
        wisdomApp.defineStream("OutputStream", "symbol", "price", "volume");

        wisdomApp.defineQuery("query1")
                .from("StockStream")
                .select("symbol", "price")
                .insertInto("OutputStream");

        this.addCallback(wisdomApp,
                map("symbol", "IBM", "price", 50.0, "volume", null),
                map("symbol", "WSO2", "price", 60.0, "volume", null));

        wisdomApp.send("StockStream", EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10));
        wisdomApp.send("StockStream", EventGenerator.generate("symbol", "WSO2", "price", 60.0, "volume", 15));

        Thread.sleep(100);

        Assert.assertEquals("Incorrect number of events", 2, eventCount.get());
    }

    @Test(expected = WisdomAppRuntimeException.class)
    public void testSelector3() throws InterruptedException {
        LOGGER.info("Test selector 3 - OUT 0");

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("StockStream", "symbol", "price", "volume");
        wisdomApp.defineStream("OutputStream", "symbol", "price", "volume");

        wisdomApp.defineQuery("query1")
                .from("StockStream")
                .select("symbol", "avg_price")
                .insertInto("OutputStream");

        wisdomApp.addExceptionListener(WisdomAppRuntimeException.class, ex -> {
            throw (WisdomAppRuntimeException) ex;
        });

        this.addCallback(wisdomApp,
                map("symbol", "IBM", "price", 50.0, "volume", null),
                map("symbol", "WSO2", "price", 60.0, "volume", null));

        wisdomApp.send("StockStream", EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10));

        Thread.sleep(100);
    }


    private void addCallback(WisdomApp wisdomApp, Map<String, Comparable>... expectedEvents) {

        wisdomApp.addCallback("OutputStream", arrivedEvents -> {

            LOGGER.info(Arrays.toString(arrivedEvents));
            int count = this.eventCount.addAndGet(arrivedEvents.length);
            if (expectedEvents.length > 0) {
                switch (count) {
                    case 1:
                        Assert.assertEquals("Incorrect event at 1", expectedEvents[0], arrivedEvents[0].getData());
                }
            }
        });
    }

    public static Map<String, Comparable> map(Comparable... entries) {
        int count = entries.length;
        Map<String, Comparable> map = new HashMap<>(count / 2);
        for (int i = 0; i < count; i += 2) {
            map.put((String) entries[i], entries[i + 1]);
        }
        return map;
    }

    @Test
    public void testPerformance() throws InterruptedException {
        LOGGER.info("Test selector 1 - OUT 2");

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("StockStream", "symbol", "price", "volume");
        wisdomApp.defineStream("OutputStream", "symbol", "price", "volume");

        wisdomApp.defineQuery("query1")
                .from("StockStream")
                .filter(event -> event.get("symbol").equals("WSO2"))
                .select("symbol", "price")
                .insertInto("OutputStream");


        wisdomApp.addCallback("OutputStream", new StreamCallback() {
            @Override
            public void receive(Event... event) {

            }
        });

        List<Long> executionTime = new ArrayList<>();
        for (int i = 0; i < 9_000_000; i++) {
            Event event = EventGenerator.generate("symbol", "WSO2", "price", 50.0, "volume", 10);
            long startTime = System.nanoTime();
            wisdomApp.send("StockStream", event);
            long endTime = System.nanoTime();
            executionTime.add(endTime - startTime);
        }

        Thread.sleep(100);

        System.out.println("Avg time: " + executionTime.stream().mapToLong(x -> x).average().getAsDouble());
    }
}
