package com.javahelps.wisdom.core.processor.pattern;

import com.javahelps.wisdom.core.TestUtil;
import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.operator.AttributeOperator;
import com.javahelps.wisdom.core.pattern.Pattern;
import com.javahelps.wisdom.core.util.EventGenerator;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.javahelps.wisdom.core.TestUtil.map;

/**
 * Created by gobinath on 6/28/17.
 */
public class CountPatternTestCase {

    private static final Logger LOGGER = LoggerFactory.getLogger(CountPatternTestCase.class);

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

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream",
                map("e1[0].price", 10.0, "e1[1].price", 20.0),
                map("e1[0].price", 30.0, "e1[1].price", 40.0));

        wisdomApp.start();

        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 10.0, "volume", 10));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 20.0, "volume", 15));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 30.0, "volume", 20));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 40.0, "volume", 25));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 30));

        Thread.sleep(100);

        Assert.assertEquals("Incorrect number of events", 2, callback.getEventCount());
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
        Pattern pattern = Pattern.followedBy(e1, e2);

        wisdomApp.defineQuery("query1")
                .from(pattern)
                .insertInto("OutputStream");

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream",
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

        Assert.assertEquals("Incorrect number of events", 2, callback.getEventCount());
    }

    @Test
    public void testPattern3() throws InterruptedException {
        LOGGER.info("Test pattern 3 - OUT 1");

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("StockStream1");
        wisdomApp.defineStream("OutputStream");

        // e1<2:5> -> e2
        Pattern e1 = Pattern.pattern("Pattern1", "e1", "StockStream1")
                .filter(event -> "IBM".equals(event.get("symbol")))
                .times(2, 5);
        Pattern e2 = Pattern.pattern("Pattern2", "e2", "StockStream1")
                .filter(event -> "WSO2".equals(event.get("symbol")));
        Pattern pattern = Pattern.followedBy(e1, e2);

        wisdomApp.defineQuery("query1")
                .from(pattern)
                .insertInto("OutputStream");

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream",
                map("e1[0].symbol", "IBM",
                        "e1[0].price", 10.0,
                        "e1[0].volume", 10,
                        "e1[1].symbol", "IBM",
                        "e1[1].price", 20.0,
                        "e1[1].volume", 15,
                        "e1[2].price", 30.0,
                        "e1[2].symbol", "IBM",
                        "e1[2].volume", 20,
                        "e1[3].price", 40.0,
                        "e1[3].symbol", "IBM",
                        "e1[3].volume", 25,
                        "e1[4].price", 50.0,
                        "e1[4].symbol", "IBM",
                        "e1[4].volume", 30,
                        "e2.symbol", "WSO2",
                        "e2.price", 80.0,
                        "e2.volume", 45));

        wisdomApp.start();

        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 10.0, "volume", 10));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 20.0, "volume", 15));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 30.0, "volume", 20));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 40.0, "volume", 25));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 30));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 60.0, "volume", 35));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 70.0, "volume", 40));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "WSO2", "price", 80.0, "volume", 45));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "WSO2", "price", 85.0, "volume", 50));

        Thread.sleep(100);

        Assert.assertEquals("Incorrect number of events", 1, callback.getEventCount());
    }

    @Test
    public void testPattern4() throws InterruptedException {
        LOGGER.info("Test pattern 4 - OUT 1");

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("StockStream1");
        wisdomApp.defineStream("OutputStream");

        // e1<2:5> -> e2
        Pattern e1 = Pattern.pattern("Pattern1", "e1", "StockStream1")
                .filter(event -> "IBM".equals(event.get("symbol")))
                .times(2, 5);
        Pattern e2 = Pattern.pattern("Pattern2", "e2", "StockStream1")
                .filter(event -> "WSO2".equals(event.get("symbol")));
        Pattern pattern = Pattern.followedBy(e1, e2);

        wisdomApp.defineQuery("query1")
                .from(pattern)
                .insertInto("OutputStream");

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream",
                map("e1[0].symbol", "IBM",
                        "e1[0].price", 10.0,
                        "e1[0].volume", 10,
                        "e1[1].symbol", "IBM",
                        "e1[1].price", 20.0,
                        "e1[1].volume", 15,
                        "e2.symbol", "WSO2",
                        "e2.price", 40.0,
                        "e2.volume", 25));

        wisdomApp.start();

        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 10.0, "volume", 10));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 20.0, "volume", 15));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "GOOGLE", "price", 30.0, "volume", 20));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "WSO2", "price", 40.0, "volume", 25));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 30));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "WSO2", "price", 60.0, "volume", 35));

        Thread.sleep(100);

        Assert.assertEquals("Incorrect number of events", 1, callback.getEventCount());
    }

    @Test
    public void testPattern5() throws InterruptedException {
        LOGGER.info("Test pattern 5 - OUT 1");

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("StockStream1");
        wisdomApp.defineStream("OutputStream");

        // e1<2:5> -> e2
        Pattern e1 = Pattern.pattern("Pattern1", "e1", "StockStream1")
                .filter(event -> "IBM".equals(event.get("symbol")))
                .times(2, 5);
        Pattern e2 = Pattern.pattern("Pattern2", "e2", "StockStream1")
                .filter(event -> "WSO2".equals(event.get("symbol")));
        Pattern pattern = Pattern.followedBy(e1, e2);

        wisdomApp.defineQuery("query1")
                .from(pattern)
                .insertInto("OutputStream");

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream",
                map("e1[0].symbol", "IBM",
                        "e1[0].price", 10.0,
                        "e1[0].volume", 10,
                        "e1[1].symbol", "IBM",
                        "e1[1].price", 30.0,
                        "e1[1].volume", 20,
                        "e2.symbol", "WSO2",
                        "e2.price", 40.0,
                        "e2.volume", 25));

        wisdomApp.start();

        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 10.0, "volume", 10));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "WSO2", "price", 20.0, "volume", 15));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 30.0, "volume", 20));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "WSO2", "price", 40.0, "volume", 25));

        Thread.sleep(100);

        Assert.assertEquals("Incorrect number of events", 1, callback.getEventCount());
    }

    @Test
    public void testPattern6() throws InterruptedException {
        LOGGER.info("Test pattern 6 - OUT 0");

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("StockStream1");
        wisdomApp.defineStream("OutputStream");

        // e1<2:5> -> e2
        Pattern e1 = Pattern.pattern("Pattern1", "e1", "StockStream1")
                .filter(event -> "IBM".equals(event.get("symbol")))
                .times(2, 5);
        Pattern e2 = Pattern.pattern("Pattern2", "e2", "StockStream1")
                .filter(event -> "WSO2".equals(event.get("symbol")));
        Pattern pattern = Pattern.followedBy(e1, e2);

        wisdomApp.defineQuery("query1")
                .from(pattern)
                .insertInto("OutputStream");

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream");

        wisdomApp.start();

        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 10.0, "volume", 10));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "WSO2", "price", 20.0, "volume", 15));

        Thread.sleep(100);

        Assert.assertEquals("Incorrect number of events", 0, callback.getEventCount());
    }

    @Test
    public void testPattern7() throws InterruptedException {
        LOGGER.info("Test pattern 7 - OUT 0");

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("StockStream1");
        wisdomApp.defineStream("OutputStream");

        // e1<0:5> -> e2
        Pattern e1 = Pattern.pattern("Pattern1", "e1", "StockStream1")
                .filter(event -> "IBM".equals(event.get("symbol")))
                .times(0, 5);
        Pattern e2 = Pattern.pattern("Pattern2", "e2", "StockStream1")
                .filter(event -> "WSO2".equals(event.get("symbol")));
        Pattern pattern = Pattern.followedBy(e1, e2);

        wisdomApp.defineQuery("query1")
                .from(pattern)
                .insertInto("OutputStream");

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream", map("e2.symbol", "WSO2", "e2.price", 10.0, "e2.volume", 10));

        wisdomApp.start();

        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "WSO2", "price", 10.0, "volume", 10));

        Thread.sleep(100);

        Assert.assertEquals("Incorrect number of events", 1, callback.getEventCount());
    }

    @Test
    public void testPattern8() throws InterruptedException {
        LOGGER.info("Test pattern 8 - OUT 1");

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("StockStream1");
        wisdomApp.defineStream("OutputStream");

        // e1 -> e2<0:5> -> e3
        Pattern e1 = Pattern.pattern("Pattern1", "e1", "StockStream1")
                .filter(AttributeOperator.attribute("price").GREATER_THAN_OR_EQUAL(50.0)
                        .and(AttributeOperator.attribute("volume").GREATER_THAN(100)));
        Pattern e2 = Pattern.pattern("Pattern2", "e2", "StockStream1")
                .filter(AttributeOperator.attribute("price").LESS_THAN_OR_EQUAL(40.0))
                .times(0, 5);
        Pattern e3 = Pattern.pattern("Pattern3", "e3", "StockStream1")
                .filter(AttributeOperator.attribute("volume").LESS_THAN_OR_EQUAL(70));

        Pattern pattern = Pattern.followedBy(Pattern.followedBy(e1, e2), e3);

        wisdomApp.defineQuery("query1")
                .from(pattern)
                .select("e1.symbol", "e2[0].symbol", "e3.symbol")
                .insertInto("OutputStream");
        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream", map("e1.symbol", "IBM", "e2[0].symbol", "GOOGLE", "e3.symbol", "WSO2"));

        wisdomApp.start();

        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 75.6, "volume", 105));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "GOOGLE", "price", 21.0, "volume", 81));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "WSO2", "price", 176.6, "volume", 65));

        Thread.sleep(100);

        Assert.assertEquals("Incorrect number of events", 1, callback.getEventCount());
    }

    @Test
    public void testPattern9() throws InterruptedException {
        LOGGER.info("Test pattern 9 - OUT 1");

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("StockStream1");
        wisdomApp.defineStream("OutputStream");

        // e1 -> e2<:5> -> e3
        Pattern e1 = Pattern.pattern("Pattern1", "e1", "StockStream1")
                .filter(AttributeOperator.attribute("price").GREATER_THAN_OR_EQUAL(50.0)
                        .and(AttributeOperator.attribute("volume").GREATER_THAN(100)));
        Pattern e2 = Pattern.pattern("Pattern2", "e2", "StockStream1")
                .filter(AttributeOperator.attribute("price").LESS_THAN_OR_EQUAL(40.0))
                .maxTimes(5);
        Pattern e3 = Pattern.pattern("Pattern3", "e3", "StockStream1")
                .filter(AttributeOperator.attribute("volume").LESS_THAN_OR_EQUAL(70));

        Pattern pattern = Pattern.followedBy(Pattern.followedBy(e1, e2), e3);

        wisdomApp.defineQuery("query1")
                .from(pattern)
                .select("e1.symbol", "e2[0].symbol", "e3.symbol")
                .insertInto("OutputStream");
        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream", map("e1.symbol", "IBM", "e3.symbol", "GOOGLE"));

        wisdomApp.start();

        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 75.6, "volume", 105));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "GOOGLE", "price", 21.0, "volume", 61));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "WSO2", "price", 176.6, "volume", 65));

        Thread.sleep(100);

        Assert.assertEquals("Incorrect number of events", 1, callback.getEventCount());
    }

    @Test
    public void testPattern10() throws InterruptedException {
        LOGGER.info("Test pattern 10 - OUT 1");

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("StockStream1");
        wisdomApp.defineStream("OutputStream");

        // e1 -> e2<:5> -> e3
        Pattern e1 = Pattern.pattern("Pattern1", "e1", "StockStream1")
                .filter(AttributeOperator.attribute("price").GREATER_THAN_OR_EQUAL(50.0)
                        .and(AttributeOperator.attribute("volume").GREATER_THAN(100)));
        Pattern e2 = Pattern.pattern("Pattern2", "e2", "StockStream1")
                .filter(AttributeOperator.attribute("price").LESS_THAN_OR_EQUAL(40.0))
                .maxTimes(5);
        Pattern e3 = Pattern.pattern("Pattern3", "e3", "StockStream1")
                .filter(AttributeOperator.attribute("volume").LESS_THAN_OR_EQUAL(70));

        Pattern pattern = Pattern.followedBy(Pattern.followedBy(e1, e2), e3);

        wisdomApp.defineQuery("query1")
                .from(pattern)
                .select("e1.symbol", "e2[0].symbol", "e2[1].symbol", "e3.symbol")
                .insertInto("OutputStream");
        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream", map("e1.symbol", "IBM", "e2[0].symbol", "GOOGLE", "e2[1].symbol", "FB",
                "e3.symbol", "WSO2"));

        wisdomApp.start();

        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 75.6, "volume", 105));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "GOOGLE", "price", 21.0, "volume", 91));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "FB", "price", 21.0, "volume", 81));
        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "WSO2", "price", 21.0, "volume", 61));

        Thread.sleep(100);

        Assert.assertEquals("Incorrect number of events", 1, callback.getEventCount());
    }

//    @Test
//    public void testPattern11() throws InterruptedException {
//        LOGGER.info("Test pattern 11 - OUT 1");
//
//        WisdomApp wisdomApp = new WisdomApp();
//        wisdomApp.defineStream("StockStream1");
//        wisdomApp.defineStream("OutputStream");
//
//        // e1 -> e2<4:6>
//        Pattern e1 = Pattern.pattern("Pattern1", "e1", "StockStream1");
//        Pattern e2 = Pattern.pattern("Pattern2", "e2", "StockStream1")
//                .filter(event -> event.get("symbol").equals(e1.event().get("symbol")))
//                .times(4, 6);
//
//        Pattern pattern = Pattern.followedBy("Pattern3", e1, e2);
//
//        wisdomApp.defineQuery("query1")
//                .from(pattern)
//                .insertInto("OutputStream");
//        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream");
//
//        wisdomApp.start();
//
//        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 75.6, "volume", 100));
//        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 75.6, "volume", 200));
//        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 75.6, "volume", 300));
//        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "GOOGLE", "price", 21.0, "volume", 91));
//        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 75.6, "volume", 400));
//        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 75.6, "volume", 500));
//
//        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "GOOGLE", "price", 21.0, "volume", 91));
//
//        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 75.6, "volume", 600));
//        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 75.6, "volume", 700));
//        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 75.6, "volume", 800));
//
//        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "GOOGLE", "price", 21.0, "volume", 91));
//        wisdomApp.send("StockStream1", EventGenerator.generate("symbol", "IBM", "price", 75.6, "volume", 900));
//
//        Thread.sleep(100);
//
//        Assert.assertEquals("Incorrect number of events", 1, callback.getEventCount());
//    }

//    @Test
//    public void testQuery13() throws InterruptedException {
//        log.info("testPatternCount13 - OUT 1");
//
//        SiddhiManager siddhiManager = new SiddhiManager();
//
//        String streams = "" +
//                "define stream EventStream (symbol string, price float, volume int); ";
//        String query = "" +
//                "@info(name = 'query1') " +
//                "from every e1 = EventStream -> " +
//                "     e2 = EventStream [e1.symbol==e2.symbol]<4:6> " +
//                "select e1.volume as volume1, e2[0].volume as volume2, e2[1].volume as volume3, e2[2].volume as " +
//                "volume4, e2[3].volume as volume5, e2[4].volume as volume6, e2[5].volume as volume7 " +
//                "insert into StockQuote;";
//
//        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
//
//        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
//            @Override
//            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
//                EventPrinter.print(timestamp, inEvents, removeEvents);
//                if (inEvents != null) {
//                    for (Event event : inEvents) {
//                        inEventCount++;
//                        switch (inEventCount) {
//                            case 1:
//                                Assert.assertArrayEquals(new Object[]{100, 200, 300, 400, 500, null, null}, event
//                                        .getData());
//                                break;
//                            case 2:
//                                Assert.assertArrayEquals(new Object[]{200, 300, 400, 500, 600, null, null}, event
//                                        .getData());
//                                break;
//                            case 3:
//                                Assert.assertArrayEquals(new Object[]{300, 400, 500, 600, 700, null, null}, event
//                                        .getData());
//                                break;
//                            case 4:
//                                Assert.assertArrayEquals(new Object[]{400, 500, 600, 700, 800, null, null}, event
//                                        .getData());
//                                break;
//                            case 5:
//                                Assert.assertArrayEquals(new Object[]{500, 600, 700, 800, 900, null, null}, event
//                                        .getData());
//                                break;
//                            default:
//                                Assert.assertSame(5, inEventCount);
//                        }
//                    }
//                }
//                if (removeEvents != null) {
//                    removeEventCount = removeEventCount + removeEvents.length;
//                }
//                eventArrived = true;
//            }
//
//        });
//
//        InputHandler eventStream = siddhiAppRuntime.getInputHandler("EventStream");
//
//        siddhiAppRuntime.start();
//
//        eventStream.send(new Object[]{"IBM", 75.6f, 100});
//        eventStream.send(new Object[]{"IBM", 75.6f, 200});
//        eventStream.send(new Object[]{"IBM", 75.6f, 300});
//        eventStream.send(new Object[]{"GOOG", 21f, 91});
//        eventStream.send(new Object[]{"IBM", 75.6f, 400});
//        eventStream.send(new Object[]{"IBM", 75.6f, 500});
//
//        eventStream.send(new Object[]{"GOOG", 21f, 91});
//
//        eventStream.send(new Object[]{"IBM", 75.6f, 600});
//        eventStream.send(new Object[]{"IBM", 75.6f, 700});
//        eventStream.send(new Object[]{"IBM", 75.6f, 800});
//        eventStream.send(new Object[]{"GOOG", 21f, 91});
//        eventStream.send(new Object[]{"IBM", 75.6f, 900});
//        Thread.sleep(100);
//
//        Assert.assertEquals("Number of success events", 5, inEventCount);
//        Assert.assertEquals("Number of remove events", 0, removeEventCount);
//        Assert.assertEquals("Event arrived", true, eventArrived);
//
//        siddhiAppRuntime.shutdown();
//    }
//
//
//    @Test
//    public void testQuery14() throws InterruptedException {
//        log.info("testPatternCount14 - OUT 1");
//
//        SiddhiManager siddhiManager = new SiddhiManager();
//
//        String streams = "" +
//                "define stream Stream1 (symbol string, price float, volume int); " +
//                "define stream Stream2 (symbol string, price float, volume int); ";
//        String query = "" +
//                "@info(name = 'query1') " +
//                "from e1=Stream1[price>20] <0:5> -> e2=Stream2[price>e1[0].price] " +
//                "select e1[0].price as price1_0, e1[1].price as price1_1, e1[2].price as price1_2, e2.price as
// price2" +
//                " " +
//                "having instanceOfFloat(e1[1].price) and not instanceOfFloat(e1[2].price) and instanceOfFloat" +
//                "(price1_1) and not instanceOfFloat(price1_2) " +
//                "insert into OutputStream ;";
//
//        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
//
//        siddhiAppRuntime.addCallback("query1", new QueryCallback() {
//            @Override
//            public void receive(long timestamp, Event[] inEvents, Event[] removeEvents) {
//                EventPrinter.print(timestamp, inEvents, removeEvents);
//                if (inEvents != null) {
//                    inEventCount = inEventCount + inEvents.length;
//                    Assert.assertArrayEquals(new Object[]{25.6f, 23.6f, null, 45.7f}, inEvents[0].getData());
//                }
//                if (removeEvents != null) {
//                    removeEventCount = removeEventCount + removeEvents.length;
//                }
//                eventArrived = true;
//            }
//
//        });
//
//        InputHandler stream1 = siddhiAppRuntime.getInputHandler("Stream1");
//        InputHandler stream2 = siddhiAppRuntime.getInputHandler("Stream2");
//
//        siddhiAppRuntime.start();
//
//        stream1.send(new Object[]{"WSO2", 25.6f, 100});
//        Thread.sleep(100);
//        stream1.send(new Object[]{"WSO2", 23.6f, 100});
//        Thread.sleep(100);
//        stream1.send(new Object[]{"GOOG", 7.6f, 100});
//        Thread.sleep(100);
//        stream2.send(new Object[]{"IBM", 45.7f, 100});
//        Thread.sleep(100);
//
//        Assert.assertEquals("Number of success events", 1, inEventCount);
//        Assert.assertEquals("Number of remove events", 0, removeEventCount);
//        Assert.assertEquals("Event arrived", true, eventArrived);
//
//        siddhiAppRuntime.shutdown();
//    }
}
