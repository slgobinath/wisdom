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
public class PatternTestCase {

    private static final Logger LOGGER = LoggerFactory.getLogger(PatternTestCase.class);
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
        wisdomApp.defineStream("StockStream2");
        wisdomApp.defineStream("OutputStream");

        // e1 -> e2 -> e3
        Pattern e1 = Pattern.pattern("Pattern1", "e1", "StockStream1")
                .filter(event -> event.get("symbol").equals("IBM"));
        Pattern e2 = Pattern.pattern("Pattern2", "e2", "StockStream2")
                .filter(event -> event.get("symbol").equals("WSO2"));
        Pattern e3 = Pattern.pattern("Pattern3", "e3", "StockStream2")
                .filter(event -> event.get("symbol").equals("ORACLE"));

        Pattern finalPattern = Pattern.followedBy("Pattern4", Pattern.followedBy("Pattern5", e1, e2), e3);


        wisdomApp.defineQuery("query1")
                .from(finalPattern)
                .select("e1.symbol", "e2.symbol", "e3.symbol")
                .insertInto("OutputStream");

        this.addCallback(wisdomApp, map("e1.symbol", "IBM", "e2.symbol", "WSO2", "e3.symbol", "ORACLE"));

        wisdomApp.start();

        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10));
        wisdomApp.send("StockStream2", EventGenerator.generate("symbol", "WSO2", "price", 50.0, "volume", 15));
        wisdomApp.send("StockStream2", EventGenerator.generate("symbol", "ORACLE", "price", 60.0, "volume", 10));

        Thread.sleep(100);

        Assert.assertEquals("Incorrect number of events", 1, eventCount.get());
    }

    @Test
    public void testPattern2() throws InterruptedException {
        LOGGER.info("Test pattern 2 - OUT 0");

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("StockStream1");
        wisdomApp.defineStream("StockStream2");
        wisdomApp.defineStream("OutputStream");

        // e1 -> e2 -> e3
        Pattern e1 = Pattern.pattern("Pattern1", "e1", "StockStream1")
                .filter(event -> event.get("symbol").equals("IBM"));
        Pattern e2 = Pattern.pattern("Pattern2", "e2", "StockStream2")
                .filter(event -> event.get("symbol").equals("WSO2"));
        Pattern e3 = Pattern.pattern("Pattern3", "e3", "StockStream2")
                .filter(event -> event.get("symbol").equals("ORACLE"));

        Pattern finalPattern = Pattern.followedBy("Pattern4", Pattern.followedBy("Pattern5", e1, e2), e3);


        wisdomApp.defineQuery("query1")
                .from(finalPattern)
                .select("e1.symbol", "e2.symbol", "e3.symbol")
                .insertInto("OutputStream");

        this.addCallback(wisdomApp);

        wisdomApp.start();

        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10));
        wisdomApp.send("StockStream2", EventGenerator.generate("symbol", "ORACLE", "price", 60.0, "volume", 10));

        Thread.sleep(100);

        Assert.assertEquals("Incorrect number of events", 0, eventCount.get());
    }

    @Test
    public void testPattern3() throws InterruptedException {
        LOGGER.info("Test pattern 1 - OUT 0");

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("StockStream1");
        wisdomApp.defineStream("StockStream2");
        wisdomApp.defineStream("OutputStream");

        // e1 -> e2 -> e3
        Pattern e1 = Pattern.pattern("Pattern1", "e1", "StockStream1")
                .filter(event -> event.get("symbol").equals("IBM"));
        Pattern e2 = Pattern.pattern("Pattern2", "e2", "StockStream2")
                .filter(event -> event.get("symbol").equals("WSO2"));
        Pattern e3 = Pattern.pattern("Pattern3", "e3", "StockStream2")
                .filter(event -> event.get("symbol").equals("ORACLE"));

        Pattern finalPattern = Pattern.followedBy("Pattern4", Pattern.followedBy("Pattern5", e1, e2), e3);


        wisdomApp.defineQuery("query1")
                .from(finalPattern)
                .select("e1.symbol", "e2.symbol", "e3.symbol")
                .insertInto("OutputStream");

        this.addCallback(wisdomApp);

        wisdomApp.start();

        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10));
        wisdomApp.send("StockStream2", EventGenerator.generate("symbol", "WSO2", "price", 50.0, "volume", 15));

        Thread.sleep(100);

        Assert.assertEquals("Incorrect number of events", 0, eventCount.get());
    }

    @Test
    public void testPattern4() throws InterruptedException {
        LOGGER.info("Test pattern 1 - OUT 0");

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("StockStream1");
        wisdomApp.defineStream("StockStream2");
        wisdomApp.defineStream("StockStream3");
        wisdomApp.defineStream("OutputStream");

        // e1 -> e2 -> e3
        Pattern e1 = Pattern.pattern("Pattern1", "e1", "StockStream1")
                .filter(event -> event.get("symbol").equals("IBM"));
        Pattern e2 = Pattern.pattern("Pattern2", "e2", "StockStream2")
                .filter(event -> event.get("symbol").equals("WSO2"));
        Pattern e3 = Pattern.pattern("Pattern3", "e3", "StockStream2")
                .filter(event -> event.get("symbol").equals("ORACLE"));

        Pattern finalPattern = Pattern.followedBy("Pattern4", Pattern.followedBy("Pattern5", e1, e2), e3);


        wisdomApp.defineQuery("query1")
                .from(finalPattern)
                .select("e1.symbol", "e2.symbol", "e3.symbol")
                .insertInto("OutputStream");

        this.addCallback(wisdomApp);

        wisdomApp.start();

        wisdomApp.send("StockStream2", EventGenerator.generate("symbol", "WSO2", "price", 50.0, "volume", 15));
        wisdomApp.send("StockStream2", EventGenerator.generate("symbol", "ORACLE", "price", 60.0, "volume", 10));

        Thread.sleep(100);

        Assert.assertEquals("Incorrect number of events", 0, eventCount.get());
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
