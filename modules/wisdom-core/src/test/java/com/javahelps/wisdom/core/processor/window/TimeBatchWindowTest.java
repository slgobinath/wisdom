package com.javahelps.wisdom.core.processor.window;

import com.javahelps.wisdom.core.TestUtil;
import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.operator.Operator;
import com.javahelps.wisdom.core.stream.InputHandler;
import com.javahelps.wisdom.core.util.EventGenerator;
import com.javahelps.wisdom.core.window.Window;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

import static com.javahelps.wisdom.core.util.Commons.map;


/**
 * Test the TimeBatchWindow of Wisdom.
 */
public class TimeBatchWindowTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(TimeBatchWindowTest.class);

    @Test
    public void testWindow1() throws InterruptedException {
        LOGGER.info("Test window 1 - OUT 2");

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("StockStream");
        wisdomApp.defineStream("OutputStream");

        wisdomApp.defineQuery("query1")
                .from("StockStream")
                .window(Window.timeBatch(Duration.ofSeconds(1)))
                .select("symbol", "price")
                .insertInto("OutputStream");

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream",
                map("symbol", "IBM", "price", 50.0),
                map("symbol", "WSO2", "price", 60.0));

        wisdomApp.start();

        InputHandler stockStreamInputHandler = wisdomApp.getInputHandler("StockStream");
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "IBM", "price", 50.0, "timestamp", 1000L));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "WSO2", "price", 60.0, "timestamp", 1500L));
        Thread.sleep(1100);
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "ORACLE", "price", 70.0, "timestamp", 2000L));
        wisdomApp.shutdown();

        Assert.assertEquals("Incorrect number of events", 2, callback.getEventCount());
    }

    @Test
    public void testWindow2() throws InterruptedException {
        LOGGER.info("Test window 2 - OUT 0");

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("StockStream");
        wisdomApp.defineStream("OutputStream");

        wisdomApp.defineQuery("query1")
                .from("StockStream")
                .window(Window.timeBatch(Duration.ofSeconds(1)))
                .select("symbol", "price")
                .insertInto("OutputStream");

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream");

        wisdomApp.start();

        InputHandler stockStreamInputHandler = wisdomApp.getInputHandler("StockStream");
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "IBM", "price", 50.0, "timestamp", 1000L));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "WSO2", "price", 60.0, "timestamp", 1500L));
        wisdomApp.shutdown();

        Assert.assertEquals("Incorrect number of events", 0, callback.getEventCount());
    }

    @Test
    public void testWindow3() throws InterruptedException {
        LOGGER.info("Test window 3 - OUT 2");

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("StockStream");
        wisdomApp.defineStream("OutputStream");

        wisdomApp.defineQuery("query1")
                .from("StockStream")
                .window(Window.timeBatch(Duration.ofSeconds(1)))
                .aggregate(Operator.SUM("price", "price"))
                .select("symbol", "price")
                .insertInto("OutputStream");

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream",
                map("symbol", "WSO2", "price", 110.0),
                map("symbol", "GOOGLE", "price", 150.0));

        wisdomApp.start();

        InputHandler stockStreamInputHandler = wisdomApp.getInputHandler("StockStream");
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "IBM", "price", 50.0, "timestamp", 1000L));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "WSO2", "price", 60.0, "timestamp", 1500L));
        Thread.sleep(1100);
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "ORACLE", "price", 70.0, "timestamp", 2000L));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "GOOGLE", "price", 80.0, "timestamp", 2500L));
        Thread.sleep(1100);
        wisdomApp.shutdown();

        Assert.assertEquals("Incorrect number of events", 2, callback.getEventCount());
    }
}
