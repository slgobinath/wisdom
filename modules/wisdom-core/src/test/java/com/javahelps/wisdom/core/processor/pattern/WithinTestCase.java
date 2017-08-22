package com.javahelps.wisdom.core.processor.pattern;

import com.javahelps.wisdom.core.TestUtil;
import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.pattern.Pattern;
import com.javahelps.wisdom.core.util.EventGenerator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

import static com.javahelps.wisdom.core.TestUtil.map;

/**
 * Test general patterns of Wisdom.
 */
public class WithinTestCase {

    private static final Logger LOGGER = LoggerFactory.getLogger(WithinTestCase.class);
    private AtomicInteger eventCount = new AtomicInteger(0);
    private TestUtil.CallbackUtil callbackUtil = new TestUtil.CallbackUtil(LOGGER, eventCount);

    @Before
    public void init() {
        this.eventCount.set(0);
    }

    @Test
    public void testWithin1() throws InterruptedException {
        LOGGER.info("Test within 1 - OUT 1");

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("StockStream1");
        wisdomApp.defineStream("StockStream2");
        wisdomApp.defineStream("OutputStream");

        // e1 -> e2 within 100 milliseconds
        Pattern e1 = Pattern.pattern("Pattern1", "e1", "StockStream1")
                .filter(event -> event.get("symbol").equals("IBM"));
        Pattern e2 = Pattern.pattern("Pattern2", "e2", "StockStream2")
                .filter(event -> event.get("symbol").equals("WSO2"));

        Pattern finalPattern = Pattern.followedBy(e1, e2, 100);


        wisdomApp.defineQuery("query1")
                .from(finalPattern)
                .select("e1.symbol", "e2.symbol", "e3.symbol")
                .insertInto("OutputStream");

        callbackUtil.addCallback(wisdomApp, map("e1.symbol", "IBM", "e2.symbol", "WSO2"));

        wisdomApp.start();

        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10));
        Thread.sleep(90);
        wisdomApp.send("StockStream2", EventGenerator.generate("symbol", "WSO2", "price", 50.0, "volume", 15));

        Thread.sleep(100);

        Assert.assertEquals("Incorrect number of events", 1, eventCount.get());
    }

    @Test
    public void testWithin2() throws InterruptedException {
        LOGGER.info("Test within 2 - OUT 0");

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("StockStream1");
        wisdomApp.defineStream("StockStream2");
        wisdomApp.defineStream("OutputStream");

        // e1 -> e2 within 100 milliseconds
        Pattern e1 = Pattern.pattern("Pattern1", "e1", "StockStream1")
                .filter(event -> event.get("symbol").equals("IBM"));
        Pattern e2 = Pattern.pattern("Pattern2", "e2", "StockStream2")
                .filter(event -> event.get("symbol").equals("WSO2"));

        Pattern finalPattern = Pattern.followedBy(e1, e2, 100);


        wisdomApp.defineQuery("query1")
                .from(finalPattern)
                .select("e1.symbol", "e2.symbol", "e3.symbol")
                .insertInto("OutputStream");

        callbackUtil.addCallback(wisdomApp);

        wisdomApp.start();

        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10));
        Thread.sleep(200);
        wisdomApp.send("StockStream2", EventGenerator.generate("symbol", "WSO2", "price", 50.0, "volume", 15));

        Thread.sleep(100);

        Assert.assertEquals("Incorrect number of events", 0, eventCount.get());
    }
}
