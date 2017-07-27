package com.javahelps.wisdom.core.processor.pattern;

import com.javahelps.wisdom.core.TestUtil;
import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.event.Event;
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
 * Test patterns with 'every' keyword.
 */
public class EveryPatternTestCase {

    private static final Logger LOGGER = LoggerFactory.getLogger(EveryPatternTestCase.class);
    private AtomicInteger eventCount = new AtomicInteger(0);
    private TestUtil.CallbackUtil callbackUtil = new TestUtil.CallbackUtil(LOGGER, eventCount);

    @Before
    public void init() {
        this.eventCount.set(0);
    }

    @Test
    public void testPattern1() throws InterruptedException {
        LOGGER.info("Test pattern 1 - OUT 2");

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("StockStream1");
        wisdomApp.defineStream("StockStream2");
        wisdomApp.defineStream("OutputStream");

        // every(e1) -> e2 -> e3
        Pattern e1 = Pattern.every(Pattern.pattern("Pattern1", "e1", "StockStream1")
                .filter(Event.attribute("price").GREATER_THAN(45.0)));
        Pattern e2 = Pattern.pattern("Pattern2", "e2", "StockStream2")
                .filter(Event.attribute("volume").GREATER_THAN(10));
        Pattern e3 = Pattern.pattern("Pattern3", "e3", "StockStream2")
                .filter(Event.attribute("price").GREATER_THAN(50.0));

        Pattern finalPattern = Pattern.followedBy(Pattern.followedBy(e1, e2), e3);


        wisdomApp.defineQuery("query1")
                .from(finalPattern)
                .select("e1.symbol", "e2.symbol", "e3.symbol")
                .insertInto("OutputStream");

        callbackUtil.addCallback(wisdomApp,
                map("e1.symbol", "IBM", "e2.symbol", "WSO2", "e3.symbol", "ORACLE"),
                map("e1.symbol", "GOOGLE", "e2.symbol", "WSO2", "e3.symbol", "ORACLE"));

        wisdomApp.start();

        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "GOOGLE", "price", 60.0, "volume", 15));
        wisdomApp.send("StockStream2", EventGenerator.generate("symbol", "WSO2", "price", 50.0, "volume", 15));
        wisdomApp.send("StockStream2", EventGenerator.generate("symbol", "ORACLE", "price", 60.0, "volume", 10));

        Thread.sleep(100);

        Assert.assertEquals("Incorrect number of events", 2, eventCount.get());
    }

    @Test
    public void testPattern2() throws InterruptedException {
        LOGGER.info("Test pattern 2 - OUT 2");

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("StockStream1");
        wisdomApp.defineStream("StockStream2");
        wisdomApp.defineStream("OutputStream");

        // e1 -> every(e2) -> e3
        Pattern e1 = Pattern.pattern("Pattern1", "e1", "StockStream1")
                .filter(Event.attribute("price").GREATER_THAN(45.0));
        Pattern e2 = Pattern.every(Pattern.pattern("Pattern2", "e2", "StockStream2")
                .filter(Event.attribute("volume").GREATER_THAN(10)));
        Pattern e3 = Pattern.pattern("Pattern3", "e3", "StockStream2")
                .filter(Event.attribute("price").GREATER_THAN(50.0));

        Pattern finalPattern = Pattern.followedBy(Pattern.followedBy(e1, e2), e3);


        wisdomApp.defineQuery("query1")
                .from(finalPattern)
                .select("e1.symbol", "e2.symbol", "e3.symbol")
                .insertInto("OutputStream");

        callbackUtil.addCallback(wisdomApp,
                map("e1.symbol", "IBM", "e2.symbol", "GOOGLE", "e3.symbol", "ORACLE"),
                map("e1.symbol", "IBM", "e2.symbol", "WSO2", "e3.symbol", "ORACLE"));

        wisdomApp.start();

        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10));
        wisdomApp.send("StockStream2", EventGenerator.generate("symbol", "GOOGLE", "price", 55.0, "volume", 15));
        wisdomApp.send("StockStream2", EventGenerator.generate("symbol", "WSO2", "price", 60.0, "volume", 20));
        wisdomApp.send("StockStream2", EventGenerator.generate("symbol", "ORACLE", "price", 65.0, "volume", 5));

        Thread.sleep(100);

        Assert.assertEquals("Incorrect number of events", 2, eventCount.get());
    }

    @Test
    public void testPattern3() throws InterruptedException {
        LOGGER.info("Test pattern 1 - OUT 0");

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("StockStream1");
        wisdomApp.defineStream("StockStream2");
        wisdomApp.defineStream("OutputStream");

        // e1 -> e2 -> every(e3)
        Pattern e1 = Pattern.pattern("Pattern1", "e1", "StockStream1")
                .filter(event -> event.get("symbol").equals("IBM"));
        Pattern e2 = Pattern.pattern("Pattern2", "e2", "StockStream2")
                .filter(event -> event.get("symbol").equals("WSO2"));
        Pattern e3 = Pattern.every(Pattern.pattern("Pattern3", "e3", "StockStream2")
                .filter(event -> event.get("symbol").equals("ORACLE")));

        Pattern finalPattern = Pattern.followedBy(Pattern.followedBy(e1, e2), e3);


        wisdomApp.defineQuery("query1")
                .from(finalPattern)
                .select("e1.symbol", "e2.symbol", "e3.symbol")
                .insertInto("OutputStream");

        callbackUtil.addCallback(wisdomApp,
                map("e1.symbol", "IBM", "e2.symbol", "WSO2", "e3.symbol", "ORACLE"),
                map("e1.symbol", "IBM", "e2.symbol", "WSO2", "e3.symbol", "ORACLE"));

        wisdomApp.start();

        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10));
        wisdomApp.send("StockStream2", EventGenerator.generate("symbol", "WSO2", "price", 50.0, "volume", 15));
        wisdomApp.send("StockStream2", EventGenerator.generate("symbol", "ORACLE", "price", 50.0, "volume", 20));
        wisdomApp.send("StockStream2", EventGenerator.generate("symbol", "ORACLE", "price", 50.0, "volume", 25));

        Thread.sleep(100);

        Assert.assertEquals("Incorrect number of events", 2, eventCount.get());
    }

//    @Test
//    public void testPattern4() throws InterruptedException {
//        LOGGER.info("Test pattern 4 - OUT 0");
//
//        WisdomApp wisdomApp = new WisdomApp();
//        wisdomApp.defineStream("StockStream1");
//        wisdomApp.defineStream("StockStream2");
//        wisdomApp.defineStream("StockStream3");
//        wisdomApp.defineStream("OutputStream");
//
//        // e1 -> e2 -> e3
//        Pattern e1 = Pattern.pattern("Pattern1", "e1", "StockStream1")
//                .filter(event -> event.get("symbol").equals("IBM"));
//        Pattern e2 = Pattern.pattern("Pattern2", "e2", "StockStream2")
//                .filter(event -> event.get("symbol").equals("WSO2"));
//        Pattern e3 = Pattern.pattern("Pattern3", "e3", "StockStream2")
//                .filter(event -> event.get("symbol").equals("ORACLE"));
//
//        Pattern finalPattern = Pattern.followedBy(Pattern.followedBy(e1, e2), e3);
//
//
//        wisdomApp.defineQuery("query1")
//                .from(finalPattern)
//                .select("e1.symbol", "e2.symbol", "e3.symbol")
//                .insertInto("OutputStream");
//
//        callbackUtil.addCallback(wisdomApp);
//
//        wisdomApp.start();
//
//        wisdomApp.send("StockStream2", EventGenerator.generate("symbol", "WSO2", "price", 50.0, "volume", 15));
//        wisdomApp.send("StockStream2", EventGenerator.generate("symbol", "ORACLE", "price", 60.0, "volume", 10));
//
//        Thread.sleep(100);
//
//        Assert.assertEquals("Incorrect number of events", 0, eventCount.get());
//    }
//
//    @Test
//    public void testPattern5() throws InterruptedException {
//        LOGGER.info("Test pattern 5 - OUT 0");
//
//        WisdomApp wisdomApp = new WisdomApp();
//        wisdomApp.defineStream("StockStream1");
//        wisdomApp.defineStream("OutputStream");
//
//        // e1 -> e2
//        Pattern e1 = Pattern.pattern("Pattern1", "e1", "StockStream1")
//                .filter(event -> event.get("symbol").equals("IBM"));
//        Pattern e2 = Pattern.pattern("Pattern2", "e2", "StockStream1")
//                .filter(event -> event.get("symbol").equals("IBM"));
//
//        Pattern finalPattern = Pattern.followedBy(e1, e2);
//
//
//        wisdomApp.defineQuery("query1")
//                .from(finalPattern)
//                .insertInto("OutputStream");
//
//        callbackUtil.addCallback(wisdomApp);
//
//        wisdomApp.start();
//
//        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10));
//
//        Thread.sleep(100);
//
//        Assert.assertEquals("Incorrect number of events", 0, eventCount.get());
//    }
//
//    @Test
//    public void testPattern6() throws InterruptedException {
//        LOGGER.info("Test pattern 6 - OUT 1");
//
//        WisdomApp wisdomApp = new WisdomApp();
//        wisdomApp.defineStream("StockStream1");
//        wisdomApp.defineStream("OutputStream");
//
//        // e1 -> e2
//        Pattern e1 = Pattern.pattern("Pattern1", "e1", "StockStream1")
//                .filter(event -> event.get("symbol").equals("IBM"));
//        Pattern e2 = Pattern.pattern("Pattern2", "e2", "StockStream1")
//                .filter(event -> event.get("symbol").equals("IBM"));
//
//        Pattern finalPattern = Pattern.followedBy(e1, e2);
//
//
//        wisdomApp.defineQuery("query1")
//                .from(finalPattern)
//                .insertInto("OutputStream");
//
//        callbackUtil.addCallback(wisdomApp, map("e1.symbol", "IBM", "e2.symbol", "IBM", "e1.price", 50.0, "e2.price", 55.0,
//                "e1.volume", 10, "e2.volume", 15));
//
//        wisdomApp.start();
//
//        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10));
//        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 55.0, "volume", 15));
//
//        Thread.sleep(100);
//
//        Assert.assertEquals("Incorrect number of events", 1, eventCount.get());
//    }
//
//    @Test
//    public void testPattern7() throws InterruptedException {
//        LOGGER.info("Test pattern 7 - OUT 1");
//
//        WisdomApp wisdomApp = new WisdomApp();
//        wisdomApp.defineStream("StockStream1");
//        wisdomApp.defineStream("OutputStream");
//
//        // e1
//        Pattern e1 = Pattern.pattern("Pattern1", "e1", "StockStream1")
//                .filter(event -> event.get("symbol").equals("IBM"));
//
//        wisdomApp.defineQuery("query1")
//                .from(e1)
//                .insertInto("OutputStream");
//
//        callbackUtil.addCallback(wisdomApp, map("e1.symbol", "IBM", "e1.price", 50.0, "e1.volume", 10)
//                , map("e1.symbol", "IBM", "e1.price", 55.0, "e1.volume", 15));
//
//        wisdomApp.start();
//
//        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10));
//        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 55.0, "volume", 15));
//
//        Thread.sleep(100);
//
//        Assert.assertEquals("Incorrect number of events", 2, eventCount.get());
//    }
}
