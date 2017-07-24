package com.javahelps.wisdom.core.processor.pattern;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.pattern.Pattern;
import com.javahelps.wisdom.core.util.EventGenerator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.javahelps.wisdom.core.TestUtil.map;

/**
 * Created by gobinath on 6/28/17.
 */
public class CountPatternTestCase {

    private static final Logger LOGGER = LoggerFactory.getLogger(CountPatternTestCase.class);
    private AtomicInteger eventCount;

    @Before
    public void init() {
        this.eventCount = new AtomicInteger(0);
    }

    @Test
    public void testPattern1() throws InterruptedException {
        LOGGER.info("Test pattern 1 - OUT 1");

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("StockStream1");
        wisdomApp.defineStream("OutputStream");

        // e1<2:5>
        Pattern pattern = Pattern.pattern("Pattern1", "e1", "StockStream1")
                .filter(event -> "IBM".equals(event.get("symbol")))
                .times(2, 5);

        wisdomApp.defineQuery("query1")
                .from(pattern)
                .select("e1[0].price", "e1[1].price")
                .insertInto("OutputStream");

        this.addCallback(wisdomApp,
                map("e1[0].price", 10.0, "e1[1].price", 20.0),
                map("e1[0].price", 30.0, "e1[1].price", 40.0));

        wisdomApp.start();

        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 10.0, "volume", 10));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 20.0, "volume", 15));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 30.0, "volume", 20));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 40.0, "volume", 25));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 30));

        Thread.sleep(100);

        Assert.assertEquals("Incorrect number of events", 2, eventCount.get());
    }

    @Test
    public void testPattern2() throws InterruptedException {
        LOGGER.info("Test pattern 2 - OUT 2");

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("StockStream1");
        wisdomApp.defineStream("OutputStream");

        // e1<2:5> -> e2
        Pattern e1 = Pattern.pattern("Pattern1", "e1", "StockStream1")
                .filter(event -> "IBM".equals(event.get("symbol")))
                .times(2, 5);
        Pattern e2 = Pattern.pattern("Pattern2", "e2", "StockStream1")
                .filter(event -> "WSO2".equals(event.get("symbol")));
        Pattern pattern = Pattern.followedBy("Pattern3", e1, e2);

        wisdomApp.defineQuery("query1")
                .from(pattern)
                .insertInto("OutputStream");

        this.addCallback(wisdomApp,
                map("e1[0].symbol", "IBM", "e1[0].price", 10.0, "e1[0].volume", 10,
                        "e1[1].symbol", "IBM", "e1[1].price", 20.0, "e1[1].volume", 15,
                        "e2.symbol", "WSO2", "e2.price", 30.0, "e2.volume", 20),
                map("e1[0].symbol", "IBM", "e1[0].price", 40.0, "e1[0].volume", 25,
                        "e1[1].symbol", "IBM", "e1[1].price", 50.0, "e1[1].volume", 30,
                        "e2.symbol", "WSO2", "e2.price", 60.0, "e2.volume", 35));

        wisdomApp.start();

        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 10.0, "volume", 10));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 20.0, "volume", 15));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "WSO2", "price", 30.0, "volume", 20));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 40.0, "volume", 25));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 30));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "WSO2", "price", 60.0, "volume", 35));

        Thread.sleep(100);

        Assert.assertEquals("Incorrect number of events", 2, eventCount.get());
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
}
