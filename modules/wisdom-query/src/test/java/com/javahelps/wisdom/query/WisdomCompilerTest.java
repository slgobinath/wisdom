package com.javahelps.wisdom.query;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.stream.InputHandler;
import com.javahelps.wisdom.core.util.EventGenerator;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.javahelps.wisdom.query.TestUtil.map;

public class WisdomCompilerTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(WisdomCompilerTest.class);

    @Test
    public void testSelectQuery() {

        LOGGER.info("Test select query");

        String query = "@app(name='WisdomApp', version='1.0.0') " +
                "def stream StockStream; " +
                "def stream OutputStream; " +
                "" +
                "from StockStream " +
                "select symbol, price " +
                "insert into OutputStream;";

        WisdomApp wisdomApp = WisdomCompiler.parse(query);

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream",
                map("symbol", "IBM", "price", 50.0),
                map("symbol", "WSO2", "price", 60.0));

        wisdomApp.start();

        InputHandler stockStreamInputHandler = wisdomApp.getInputHandler("StockStream");
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "WSO2", "price", 60.0, "volume", 15));

        wisdomApp.shutdown();

        Assert.assertEquals("Incorrect number of events", 2, callback.getEventCount());
    }
}
