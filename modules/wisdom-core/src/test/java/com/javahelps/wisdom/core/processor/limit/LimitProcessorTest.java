package com.javahelps.wisdom.core.processor.limit;

import com.javahelps.wisdom.core.TestUtil;
import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.stream.InputHandler;
import com.javahelps.wisdom.core.util.EventGenerator;
import com.javahelps.wisdom.core.window.Window;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

import static com.javahelps.wisdom.core.util.Commons.map;

public class LimitProcessorTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(LimitProcessorTest.class);

    @Test
    public void testLimit1() throws InterruptedException {
        LOGGER.info("Test limit 1 - OUT 2");

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("StockStream");
        wisdomApp.defineStream("OutputStream");

        wisdomApp.defineQuery("query1")
                .from("StockStream")
                .window(Window.lengthBatch(3))
                .limit(2)
                .select("symbol", "price")
                .insertInto("OutputStream");

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream",
                map("symbol", "IBM", "price", 50.0),
                map("symbol", "WSO2", "price", 60.0));

        wisdomApp.start();

        InputHandler stockStreamInputHandler = wisdomApp.getInputHandler("StockStream");
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "WSO2", "price", 60.0, "volume", 15));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "ORACLE", "price", 70.0, "volume", 20));

        Thread.sleep(100);
        wisdomApp.shutdown();

        Assert.assertEquals("Incorrect number of events", 2, callback.getEventCount());
    }

    @Test
    public void testLimit2() throws InterruptedException {
        LOGGER.info("Test limit 2 - OUT 4");

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("StockStream");
        wisdomApp.defineStream("OutputStream");

        wisdomApp.defineQuery("query1")
                .from("StockStream")
                .window(Window.externalTimeBatch("timestamp", Duration.ofSeconds(1)))
                .limit(2, 4, 6)
                .select("symbol", "price")
                .insertInto("OutputStream");

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream",
                map("symbol", "IBM", "price", 50.0),
                map("symbol", "WSO2", "price", 60.0),
                map("symbol", "ORACLE", "price", 70.0),
                map("symbol", "GOOGLE", "price", 80.0));

        wisdomApp.start();

        InputHandler stockStreamInputHandler = wisdomApp.getInputHandler("StockStream");
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "IBM", "price", 50.0, "timestamp", 1000L));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "WSO2", "price", 60.0, "timestamp", 1100L));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "ORACLE", "price", 70.0, "timestamp", 1200L));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "GOOGLE", "price", 80.0, "timestamp", 1300L));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "IBM", "price", 90.0, "timestamp", 1400L));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "AMAZON", "price", 100.0, "timestamp", 2500L));

        Thread.sleep(100);

        Assert.assertEquals("Incorrect number of events", 4, callback.getEventCount());
    }
}
