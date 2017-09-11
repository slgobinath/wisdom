package com.javahelps.wisdom.core.processor.window;

import com.javahelps.wisdom.core.TestUtil;
import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.operator.AttributeOperator;
import com.javahelps.wisdom.core.stream.InputHandler;
import com.javahelps.wisdom.core.util.EventGenerator;
import com.javahelps.wisdom.core.window.Window;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

import static com.javahelps.wisdom.core.TestUtil.map;

/**
 * Test the LengthBatchWindow of Wisdom.
 */
public class LengthBatchWindowTestCase {

    private static final Logger LOGGER = LoggerFactory.getLogger(LengthBatchWindowTestCase.class);
    private AtomicInteger eventCount = new AtomicInteger(0);
    private TestUtil.CallbackUtil callbackUtil = new TestUtil.CallbackUtil(LOGGER, eventCount);

    @Before
    public void init() {
        this.eventCount.set(0);
    }

    @Test
    public void testWindow1() throws InterruptedException {
        LOGGER.info("Test window 1 - OUT 3");

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("StockStream");
        wisdomApp.defineStream("OutputStream");

        wisdomApp.defineQuery("query1")
                .from("StockStream")
                .window(Window.lengthBatch(3))
                .select("symbol", "price")
                .insertInto("OutputStream");

        callbackUtil.addCallback(wisdomApp,
                map("symbol", "IBM", "price", 50.0),
                map("symbol", "WSO2", "price", 60.0),
                map("symbol", "ORACLE", "price", 70.0));

        wisdomApp.start();

        InputHandler stockStreamInputHandler = wisdomApp.getInputHandler("StockStream");
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "WSO2", "price", 60.0, "volume", 15));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "ORACLE", "price", 70.0, "volume", 20));

        Thread.sleep(100);

        Assert.assertEquals("Incorrect number of events", 3, eventCount.get());
    }

    @Test
    public void testWindow2() throws InterruptedException {
        LOGGER.info("Test window 2 - OUT 0");

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("StockStream");
        wisdomApp.defineStream("OutputStream");

        wisdomApp.defineQuery("query1")
                .from("StockStream")
                .window(Window.lengthBatch(3))
                .select("symbol", "price")
                .insertInto("OutputStream");

        callbackUtil.addCallback(wisdomApp);

        wisdomApp.start();

        InputHandler stockStreamInputHandler = wisdomApp.getInputHandler("StockStream");
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "WSO2", "price", 60.0, "volume", 15));

        Thread.sleep(100);

        Assert.assertEquals("Incorrect number of events", 0, eventCount.get());
    }

    @Test
    public void testWindow3() throws InterruptedException {
        LOGGER.info("Test window 3 - OUT 3");

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("StockStream");
        wisdomApp.defineStream("OutputStream");

        wisdomApp.defineQuery("query1")
                .from("StockStream")
                .filter(AttributeOperator.attribute("price").GREATER_THAN(55.0))
                .window(Window.lengthBatch(3))
                .select("symbol", "price")
                .insertInto("OutputStream");

        callbackUtil.addCallback(wisdomApp,
                map("symbol", "WSO2", "price", 60.0),
                map("symbol", "ORACLE", "price", 70.0),
                map("symbol", "GOOGLE", "price", 80.0));

        wisdomApp.start();

        InputHandler stockStreamInputHandler = wisdomApp.getInputHandler("StockStream");
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "WSO2", "price", 60.0, "volume", 15));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "ORACLE", "price", 70.0, "volume", 20));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "GOOGLE", "price", 80.0, "volume", 25));

        Thread.sleep(100);

        Assert.assertEquals("Incorrect number of events", 3, eventCount.get());
    }
}
