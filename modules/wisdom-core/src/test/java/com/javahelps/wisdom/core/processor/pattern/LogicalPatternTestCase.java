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

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.javahelps.wisdom.core.TestUtil.map;

/**
 * Test logical patterns of Wisdom which includes AND, OR & NOT.
 */
public class LogicalPatternTestCase {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogicalPatternTestCase.class);
    private AtomicInteger eventCount = new AtomicInteger(0);
    private TestUtil.CallbackUtil callbackUtil = new TestUtil.CallbackUtil(LOGGER, eventCount);

    @Before
    public void init() {
        this.eventCount.set(0);
    }

    @Test
    public void testPattern1() throws InterruptedException {
        LOGGER.info("Test pattern 1 - OUT 1");

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("StockStream1");
        wisdomApp.defineStream("StockStream2");
        wisdomApp.defineStream("StockStream3");
        wisdomApp.defineStream("OutputStream");

        // e1 and e2 -> e3
        Pattern e1 = Pattern.pattern("Pattern1", "e1", "StockStream1")
                .filter(event -> event.get("symbol").equals("IBM"));
        Pattern e2 = Pattern.pattern("Pattern2", "e2", "StockStream2")
                .filter(event -> event.get("symbol").equals("WSO2"));
        Pattern e3 = Pattern.pattern("Pattern3", "e3", "StockStream3")
                .filter(event -> event.get("symbol").equals("ORACLE"));

        Pattern finalPattern = Pattern.followedBy(Pattern.and(e1, e2), e3);

        wisdomApp.defineQuery("query1")
                .from(finalPattern)
                .select("e1.symbol", "e2.symbol", "e3.symbol")
                .insertInto("OutputStream");

        callbackUtil.addCallback(wisdomApp, map("e1.symbol", "IBM", "e2.symbol", "WSO2", "e3.symbol", "ORACLE"));

        wisdomApp.start();

        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10));
        wisdomApp.send("StockStream2", EventGenerator.generate("symbol", "WSO2", "price", 50.0, "volume", 15));
        wisdomApp.send("StockStream3", EventGenerator.generate("symbol", "ORACLE", "price", 60.0, "volume", 10));

        Thread.sleep(100);

        wisdomApp.shutdown();

        Assert.assertEquals("Incorrect number of events", 1, eventCount.get());
    }

    @Test
    public void testPattern2() throws InterruptedException {
        LOGGER.info("Test pattern 2 - OUT 0");

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("StockStream1");
        wisdomApp.defineStream("StockStream2");
        wisdomApp.defineStream("StockStream3");
        wisdomApp.defineStream("OutputStream");

        // e1 and e2 -> e3
        Pattern e1 = Pattern.pattern("Pattern1", "e1", "StockStream1")
                .filter(event -> event.get("symbol").equals("IBM"));
        Pattern e2 = Pattern.pattern("Pattern2", "e2", "StockStream2")
                .filter(event -> event.get("symbol").equals("WSO2"));
        Pattern e3 = Pattern.pattern("Pattern3", "e3", "StockStream3")
                .filter(event -> event.get("symbol").equals("ORACLE"));

        Pattern finalPattern = Pattern.followedBy(Pattern.and(e1, e2), e3);

        wisdomApp.defineQuery("query1")
                .from(finalPattern)
                .select("e1.symbol", "e2.symbol", "e3.symbol")
                .insertInto("OutputStream");

        callbackUtil.addCallback(wisdomApp);

        wisdomApp.start();

        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10));
        wisdomApp.send("StockStream2", EventGenerator.generate("symbol", "ORACLE", "price", 60.0, "volume", 10));

        Thread.sleep(100);

        wisdomApp.shutdown();

        Assert.assertEquals("Incorrect number of events", 0, eventCount.get());
    }

    @Test
    public void testPattern3() throws InterruptedException {
        LOGGER.info("Test pattern 3 - OUT 0");

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("StockStream1");
        wisdomApp.defineStream("StockStream2");
        wisdomApp.defineStream("StockStream3");
        wisdomApp.defineStream("OutputStream");

        // e1 and e2 -> e3
        Pattern e1 = Pattern.pattern("Pattern1", "e1", "StockStream1")
                .filter(event -> event.get("symbol").equals("IBM"));
        Pattern e2 = Pattern.pattern("Pattern2", "e2", "StockStream2")
                .filter(event -> event.get("symbol").equals("WSO2"));
        Pattern e3 = Pattern.pattern("Pattern3", "e3", "StockStream3")
                .filter(event -> event.get("symbol").equals("ORACLE"));

        Pattern finalPattern = Pattern.followedBy(Pattern.and(e1, e2), e3);

        wisdomApp.defineQuery("query1")
                .from(finalPattern)
                .select("e1.symbol", "e2.symbol", "e3.symbol")
                .insertInto("OutputStream");

        callbackUtil.addCallback(wisdomApp);

        wisdomApp.start();

        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10));
        wisdomApp.send("StockStream2", EventGenerator.generate("symbol", "WSO2", "price", 50.0, "volume", 15));

        Thread.sleep(100);

        wisdomApp.shutdown();

        Assert.assertEquals("Incorrect number of events", 0, eventCount.get());
    }

    @Test
    public void testPattern4() throws InterruptedException {
        LOGGER.info("Test pattern 4 - OUT 0");

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("StockStream1");
        wisdomApp.defineStream("StockStream2");
        wisdomApp.defineStream("StockStream3");
        wisdomApp.defineStream("OutputStream");

        // e1 and e2 -> e3
        Pattern e1 = Pattern.pattern("Pattern1", "e1", "StockStream1")
                .filter(event -> event.get("symbol").equals("IBM"));
        Pattern e2 = Pattern.pattern("Pattern2", "e2", "StockStream2")
                .filter(event -> event.get("symbol").equals("WSO2"));
        Pattern e3 = Pattern.pattern("Pattern3", "e3", "StockStream3")
                .filter(event -> event.get("symbol").equals("ORACLE"));

        Pattern finalPattern = Pattern.followedBy(Pattern.and(e1, e2), e3);

        wisdomApp.defineQuery("query1")
                .from(finalPattern)
                .select("e1.symbol", "e2.symbol", "e3.symbol")
                .insertInto("OutputStream");

        callbackUtil.addCallback(wisdomApp);

        wisdomApp.start();

        wisdomApp.send("StockStream2", EventGenerator.generate("symbol", "WSO2", "price", 50.0, "volume", 15));
        wisdomApp.send("StockStream2", EventGenerator.generate("symbol", "ORACLE", "price", 60.0, "volume", 10));

        Thread.sleep(100);

        wisdomApp.shutdown();

        Assert.assertEquals("Incorrect number of events", 0, eventCount.get());
    }

    @Test
    public void testPattern5() throws InterruptedException {
        LOGGER.info("Test pattern 5 - OUT 1");

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("StockStream1");
        wisdomApp.defineStream("StockStream2");
        wisdomApp.defineStream("StockStream3");
        wisdomApp.defineStream("OutputStream");

        // e1 or e2 -> e3
        Pattern e1 = Pattern.pattern("Pattern1", "e1", "StockStream1")
                .filter(event -> event.get("symbol").equals("IBM"));
        Pattern e2 = Pattern.pattern("Pattern2", "e2", "StockStream2")
                .filter(event -> event.get("symbol").equals("WSO2"));
        Pattern e3 = Pattern.pattern("Pattern3", "e3", "StockStream3")
                .filter(event -> event.get("symbol").equals("ORACLE"));

        Pattern finalPattern = Pattern.followedBy(Pattern.or(e1, e2), e3);

        wisdomApp.defineQuery("query1")
                .from(finalPattern)
                .select("e1.symbol", "e2.symbol", "e3.symbol")
                .insertInto("OutputStream");

        callbackUtil.addCallback(wisdomApp, map("e1.symbol", "IBM", "e3.symbol", "ORACLE"));

        wisdomApp.start();

        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10));
        wisdomApp.send("StockStream2", EventGenerator.generate("symbol", "WSO2", "price", 50.0, "volume", 15));
        wisdomApp.send("StockStream3", EventGenerator.generate("symbol", "ORACLE", "price", 60.0, "volume", 10));

        Thread.sleep(100);

        wisdomApp.shutdown();

        Assert.assertEquals("Incorrect number of events", 1, eventCount.get());
    }

    @Test
    public void testPattern6() throws InterruptedException {
        LOGGER.info("Test pattern 6 - OUT 1");

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("StockStream1");
        wisdomApp.defineStream("StockStream2");
        wisdomApp.defineStream("StockStream3");
        wisdomApp.defineStream("OutputStream");

        // e1 or e2 -> e3
        Pattern e1 = Pattern.pattern("Pattern1", "e1", "StockStream1")
                .filter(event -> event.get("symbol").equals("IBM"));
        Pattern e2 = Pattern.pattern("Pattern2", "e2", "StockStream2")
                .filter(event -> event.get("symbol").equals("WSO2"));
        Pattern e3 = Pattern.pattern("Pattern3", "e3", "StockStream3")
                .filter(event -> event.get("symbol").equals("ORACLE"));

        Pattern finalPattern = Pattern.followedBy(Pattern.or(e1, e2), e3);

        wisdomApp.defineQuery("query1")
                .from(finalPattern)
                .select("e1.symbol", "e2.symbol", "e3.symbol")
                .insertInto("OutputStream");

        callbackUtil.addCallback(wisdomApp, map("e1.symbol", "IBM", "e3.symbol", "ORACLE"));

        wisdomApp.start();

        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10));
        wisdomApp.send("StockStream3", EventGenerator.generate("symbol", "ORACLE", "price", 60.0, "volume", 10));

        Thread.sleep(100);

        wisdomApp.shutdown();

        Assert.assertEquals("Incorrect number of events", 1, eventCount.get());
    }

    @Test
    public void testPattern7() throws InterruptedException {
        LOGGER.info("Test pattern 7 - OUT 1");

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("StockStream1");
        wisdomApp.defineStream("StockStream2");
        wisdomApp.defineStream("StockStream3");
        wisdomApp.defineStream("OutputStream");

        // e1 or e2 -> e3
        Pattern e1 = Pattern.pattern("Pattern1", "e1", "StockStream1")
                .filter(event -> event.get("symbol").equals("IBM"));
        Pattern e2 = Pattern.pattern("Pattern2", "e2", "StockStream2")
                .filter(event -> event.get("symbol").equals("WSO2"));
        Pattern e3 = Pattern.pattern("Pattern3", "e3", "StockStream3")
                .filter(event -> event.get("symbol").equals("ORACLE"));

        Pattern finalPattern = Pattern.followedBy(Pattern.or(e1, e2), e3);

        wisdomApp.defineQuery("query1")
                .from(finalPattern)
                .select("e1.symbol", "e2.symbol", "e3.symbol")
                .insertInto("OutputStream");

        callbackUtil.addCallback(wisdomApp, map("e2.symbol", "WSO2", "e3.symbol", "ORACLE"));

        wisdomApp.start();

        wisdomApp.send("StockStream2", EventGenerator.generate("symbol", "WSO2", "price", 50.0, "volume", 15));
        wisdomApp.send("StockStream3", EventGenerator.generate("symbol", "ORACLE", "price", 60.0, "volume", 10));

        Thread.sleep(100);

        wisdomApp.shutdown();

        Assert.assertEquals("Incorrect number of events", 1, eventCount.get());
    }

    @Test
    public void testPattern8() throws InterruptedException {
        LOGGER.info("Test pattern 8 - OUT 0");

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("StockStream1");
        wisdomApp.defineStream("StockStream2");
        wisdomApp.defineStream("StockStream3");
        wisdomApp.defineStream("OutputStream");

        // e1 or e2 -> e3
        Pattern e1 = Pattern.pattern("Pattern1", "e1", "StockStream1")
                .filter(event -> event.get("symbol").equals("IBM"));
        Pattern e2 = Pattern.pattern("Pattern2", "e2", "StockStream2")
                .filter(event -> event.get("symbol").equals("WSO2"));
        Pattern e3 = Pattern.pattern("Pattern3", "e3", "StockStream3")
                .filter(event -> event.get("symbol").equals("ORACLE"));

        Pattern finalPattern = Pattern.followedBy(Pattern.or(e1, e2), e3);

        wisdomApp.defineQuery("query1")
                .from(finalPattern)
                .select("e1.symbol", "e2.symbol", "e3.symbol")
                .insertInto("OutputStream");

        callbackUtil.addCallback(wisdomApp);

        wisdomApp.start();

        wisdomApp.send("StockStream3", EventGenerator.generate("symbol", "ORACLE", "price", 60.0, "volume", 10));

        Thread.sleep(100);

        wisdomApp.shutdown();

        Assert.assertEquals("Incorrect number of events", 0, eventCount.get());

    }

    @Test
    public void testPattern9() throws InterruptedException {
        LOGGER.info("Test pattern 9 - OUT 1");

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("StockStream1");
        wisdomApp.defineStream("StockStream2");
        wisdomApp.defineStream("StockStream3");
        wisdomApp.defineStream("StockStream4");
        wisdomApp.defineStream("OutputStream");

        // (e1 and e2) or e3 -> e4
        Pattern e1 = Pattern.pattern("Pattern1", "e1", "StockStream1")
                .filter(event -> event.get("symbol").equals("IBM"));
        Pattern e2 = Pattern.pattern("Pattern2", "e2", "StockStream2")
                .filter(event -> event.get("symbol").equals("WSO2"));
        Pattern e3 = Pattern.pattern("Pattern3", "e3", "StockStream3")
                .filter(event -> event.get("symbol").equals("ORACLE"));
        Pattern e4 = Pattern.pattern("Pattern4", "e4", "StockStream4")
                .filter(event -> event.get("symbol").equals("MICROSOFT"));

        Pattern finalPattern = Pattern.followedBy(Pattern.or(Pattern.and(e1, e2), e3), e4);

        wisdomApp.defineQuery("query1")
                .from(finalPattern)
                .select("e1.symbol", "e2.symbol", "e3.symbol", "e4.symbol")
                .insertInto("OutputStream");

        callbackUtil.addCallback(wisdomApp, map("e1.symbol", "IBM", "e2.symbol", "WSO2", "e4.symbol", "MICROSOFT"));

        wisdomApp.start();

        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10));
        wisdomApp.send("StockStream2", EventGenerator.generate("symbol", "WSO2", "price", 50.0, "volume", 15));
        wisdomApp.send("StockStream3", EventGenerator.generate("symbol", "ORACLE", "price", 60.0, "volume", 10));
        wisdomApp.send("StockStream4", EventGenerator.generate("symbol", "MICROSOFT", "price", 60.0, "volume", 10));

        wisdomApp.shutdown();

        Assert.assertEquals("Incorrect number of events", 1, eventCount.get());
    }

    @Test
    public void testPattern10() throws InterruptedException {
        LOGGER.info("Test pattern 10 - OUT 1");

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("StockStream1");
        wisdomApp.defineStream("StockStream2");
        wisdomApp.defineStream("OutputStream");

        // not e1 -> e2
        Pattern e1 = Pattern.pattern("Pattern1", "e1", "StockStream1")
                .filter(event -> event.get("symbol").equals("IBM"));
        Pattern e2 = Pattern.pattern("Pattern2", "e2", "StockStream2")
                .filter(event -> event.get("symbol").equals("WSO2"));

        Pattern finalPattern = Pattern.followedBy(Pattern.not(e1), e2);

        wisdomApp.defineQuery("query1")
                .from(finalPattern)
                .select("e1.symbol", "e2.symbol")
                .insertInto("OutputStream");

        callbackUtil.addCallback(wisdomApp, map("e2.symbol", "WSO2"));

        wisdomApp.start();

        wisdomApp.send("StockStream2", EventGenerator.generate("symbol", "WSO2", "price", 50.0, "volume", 15));

        wisdomApp.shutdown();

        Assert.assertEquals("Incorrect number of events", 1, eventCount.get());
    }

    @Test
    public void testPattern11() throws InterruptedException {
        LOGGER.info("Test pattern 11 - OUT 0");

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("StockStream1");
        wisdomApp.defineStream("StockStream2");
        wisdomApp.defineStream("OutputStream");

        // not e1 -> e2
        Pattern e1 = Pattern.pattern("Pattern1", "e1", "StockStream1")
                .filter(event -> event.get("symbol").equals("IBM"));
        Pattern e2 = Pattern.pattern("Pattern2", "e2", "StockStream2")
                .filter(event -> event.get("symbol").equals("WSO2"));

        Pattern finalPattern = Pattern.followedBy(Pattern.not(e1), e2);

        wisdomApp.defineQuery("query1")
                .from(finalPattern)
                .select("e1.symbol", "e2.symbol")
                .insertInto("OutputStream");

        callbackUtil.addCallback(wisdomApp);

        wisdomApp.start();

        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10));
        wisdomApp.send("StockStream2", EventGenerator.generate("symbol", "WSO2", "price", 50.0, "volume", 15));

        wisdomApp.shutdown();

        Assert.assertEquals("Incorrect number of events", 0, eventCount.get());
    }

    @Test
    public void testPattern12() throws InterruptedException {
        LOGGER.info("Test pattern 12 - OUT 1");

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("StockStream1");
        wisdomApp.defineStream("StockStream2");
        wisdomApp.defineStream("OutputStream");

        // e1 -> not e2
        Pattern e1 = Pattern.pattern("Pattern1", "e1", "StockStream1")
                .filter(event -> event.get("symbol").equals("IBM"));
        Pattern e2 = Pattern.pattern("Pattern2", "e2", "StockStream2")
                .filter(event -> event.get("symbol").equals("WSO2"));

        Pattern finalPattern = Pattern.followedBy(e1, Pattern.not(e2).within(Duration.of(1, ChronoUnit.SECONDS)));

        wisdomApp.defineQuery("query1")
                .from(finalPattern)
                .select("e1.symbol", "e2.symbol")
                .insertInto("OutputStream");

        callbackUtil.addCallback(wisdomApp, map("e1.symbol", "IBM"));

        wisdomApp.start();


        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10));

        Thread.sleep(1100);

        wisdomApp.shutdown();

        Assert.assertEquals("Incorrect number of events", 1, eventCount.get());
    }

    @Test
    public void testPattern13() throws InterruptedException {
        LOGGER.info("Test pattern 13 - OUT 1");

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("StockStream1");
        wisdomApp.defineStream("StockStream2");
        wisdomApp.defineStream("StockStream3");
        wisdomApp.defineStream("OutputStream");

        // e1 -> not e2 -> e3
        Pattern e1 = Pattern.pattern("Pattern1", "e1", "StockStream1")
                .filter(event -> event.get("symbol").equals("IBM"));
        Pattern e2 = Pattern.pattern("Pattern2", "e2", "StockStream2")
                .filter(event -> event.get("symbol").equals("WSO2"));
        Pattern e3 = Pattern.pattern("Pattern3", "e3", "StockStream3")
                .filter(event -> ((Double) event.get("price")).doubleValue() >= ((Double) e1.event().get("price"))
                        .doubleValue());

        Pattern finalPattern = Pattern.followedBy(Pattern.followedBy(e1, Pattern.not(e2)), e3);

        wisdomApp.defineQuery("query1")
                .from(finalPattern)
                .select("e1.symbol", "e3.symbol")
                .insertInto("OutputStream");

        callbackUtil.addCallback(wisdomApp, map("e1.symbol", "IBM", "e3.symbol", "WSO2"));

        wisdomApp.start();


        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10));
        wisdomApp.send("StockStream3", EventGenerator.generate("symbol", "WSO2", "price", 60.0, "volume", 15));

        Thread.sleep(100);

        wisdomApp.shutdown();

        Assert.assertEquals("Incorrect number of events", 1, eventCount.get());
    }

    @Test
    public void testPattern14() throws InterruptedException {
        LOGGER.info("Test pattern 14 - OUT 0");

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("StockStream1");
        wisdomApp.defineStream("StockStream2");
        wisdomApp.defineStream("StockStream3");
        wisdomApp.defineStream("OutputStream");

        // e1 -> not e2 -> e3
        Pattern e1 = Pattern.pattern("Pattern1", "e1", "StockStream1")
                .filter(event -> event.get("symbol").equals("IBM"));
        Pattern e2 = Pattern.pattern("Pattern2", "e2", "StockStream2")
                .filter(event -> event.get("symbol").equals("WSO2"));
        Pattern e3 = Pattern.pattern("Pattern3", "e3", "StockStream3")
                .filter(event -> ((Double) event.get("price")).doubleValue() >= ((Double) e1.event().get("price"))
                        .doubleValue());

        Pattern finalPattern = Pattern.followedBy(Pattern.followedBy(e1, Pattern.not(e2)), e3);

        wisdomApp.defineQuery("query1")
                .from(finalPattern)
                .select("e1.symbol", "e3.symbol")
                .insertInto("OutputStream");

        callbackUtil.addCallback(wisdomApp);

        wisdomApp.start();


        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10));
        wisdomApp.send("StockStream2", EventGenerator.generate("symbol", "WSO2", "price", 55.0, "volume", 12));
        wisdomApp.send("StockStream3", EventGenerator.generate("symbol", "WSO2", "price", 60.0, "volume", 15));

        Thread.sleep(100);

        wisdomApp.shutdown();

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
