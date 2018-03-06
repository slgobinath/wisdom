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

    @Test
    public void testGreaterThanFilterQuery() {

        LOGGER.info("Test greater than filter query");

        String query = "@app(name='WisdomApp', version='1.0.0') " +
                "def stream StockStream; " +
                "def stream OutputStream; " +
                "" +
                "from StockStream " +
                "filter volume > 10 " +
                "select symbol, price " +
                "insert into OutputStream;";

        WisdomApp wisdomApp = WisdomCompiler.parse(query);

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream",
                map("symbol", "WSO2", "price", 60.0),
                map("symbol", "ORACLE", "price", 70.0));

        wisdomApp.start();

        InputHandler stockStreamInputHandler = wisdomApp.getInputHandler("StockStream");
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "WSO2", "price", 60.0, "volume", 15));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "ORACLE", "price", 70.0, "volume", 25));

        wisdomApp.shutdown();

        Assert.assertEquals("Incorrect number of events", 2, callback.getEventCount());
    }

    @Test
    public void testLessThanFilterQuery() {

        LOGGER.info("Test less than filter query");

        String query = "@app(name='WisdomApp', version='1.0.0') " +
                "def stream StockStream; " +
                "def stream OutputStream; " +
                "" +
                "from StockStream " +
                "filter volume < 15 " +
                "select symbol, price " +
                "insert into OutputStream;";

        WisdomApp wisdomApp = WisdomCompiler.parse(query);

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream",
                map("symbol", "IBM", "price", 50.0));

        wisdomApp.start();

        InputHandler stockStreamInputHandler = wisdomApp.getInputHandler("StockStream");
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "WSO2", "price", 60.0, "volume", 15));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "ORACLE", "price", 70.0, "volume", 25));

        wisdomApp.shutdown();

        Assert.assertEquals("Incorrect number of events", 1, callback.getEventCount());
    }

    @Test
    public void testEqualsFilterQuery() {

        LOGGER.info("Test equals filter query");

        String query = "@app(name='WisdomApp', version='1.0.0') " +
                "def stream StockStream; " +
                "def stream OutputStream; " +
                "" +
                "from StockStream " +
                "filter symbol == 'WSO2' " +
                "select symbol, price " +
                "insert into OutputStream;";

        WisdomApp wisdomApp = WisdomCompiler.parse(query);

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream",
                map("symbol", "WSO2", "price", 60.0));

        wisdomApp.start();

        InputHandler stockStreamInputHandler = wisdomApp.getInputHandler("StockStream");
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "WSO2", "price", 60.0, "volume", 15));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "ORACLE", "price", 70.0, "volume", 25));

        wisdomApp.shutdown();

        Assert.assertEquals("Incorrect number of events", 1, callback.getEventCount());
    }

    @Test
    public void testNotEqualsFilterQuery() {

        LOGGER.info("Test not equals filter query");

        String query = "@app(name='WisdomApp', version='1.0.0') " +
                "def stream StockStream; " +
                "def stream OutputStream; " +
                "" +
                "from StockStream " +
                "filter not symbol == 'WSO2' " +
                "select symbol, price " +
                "insert into OutputStream;";

        WisdomApp wisdomApp = WisdomCompiler.parse(query);

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream",
                map("symbol", "IBM", "price", 50.0),
                map("symbol", "ORACLE", "price", 70.0));

        wisdomApp.start();

        InputHandler stockStreamInputHandler = wisdomApp.getInputHandler("StockStream");
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "WSO2", "price", 60.0, "volume", 15));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "ORACLE", "price", 70.0, "volume", 25));

        wisdomApp.shutdown();

        Assert.assertEquals("Incorrect number of events", 2, callback.getEventCount());
    }

    @Test
    public void testANDFilterQuery() {

        LOGGER.info("Test AND filter query");

        String query = "@app(name='WisdomApp', version='1.0.0') " +
                "def stream StockStream; " +
                "def stream OutputStream; " +
                "" +
                "from StockStream " +
                "filter symbol == 'WSO2' and price > 50 " +
                "select symbol, price " +
                "insert into OutputStream;";

        WisdomApp wisdomApp = WisdomCompiler.parse(query);

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream",
                map("symbol", "WSO2", "price", 60.0));

        wisdomApp.start();

        InputHandler stockStreamInputHandler = wisdomApp.getInputHandler("StockStream");
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "WSO2", "price", 50.0, "volume", 10));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "WSO2", "price", 60.0, "volume", 15));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "ORACLE", "price", 70.0, "volume", 25));

        wisdomApp.shutdown();

        Assert.assertEquals("Incorrect number of events", 1, callback.getEventCount());
    }

    @Test
    public void testORFilterQuery() {

        LOGGER.info("Test OR filter query");

        String query = "@app(name='WisdomApp', version='1.0.0') " +
                "def stream StockStream; " +
                "def stream OutputStream; " +
                "" +
                "from StockStream " +
                "filter symbol == 'WSO2' and price > 50 or volume == 25 " +
                "select symbol, price " +
                "insert into OutputStream;";

        WisdomApp wisdomApp = WisdomCompiler.parse(query);

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream",
                map("symbol", "WSO2", "price", 10.0),
                map("symbol", "WSO2", "price", 60.0),
                map("symbol", "ORACLE", "price", 20.0));

        wisdomApp.start();

        InputHandler stockStreamInputHandler = wisdomApp.getInputHandler("StockStream");
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "WSO2", "price", 10.0, "volume", 25.0));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "WSO2", "price", 50.0, "volume", 10.0));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "WSO2", "price", 60.0, "volume", 15.0));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "ORACLE", "price", 20.0, "volume", 25.0));

        wisdomApp.shutdown();

        Assert.assertEquals("Incorrect number of events", 3, callback.getEventCount());
    }

    @Test
    public void testOperatorPrecedenceFilterQuery() {

        LOGGER.info("Test logical operator precedence query");

        String query = "@app(name='WisdomApp', version='1.0.0') " +
                "def stream StockStream; " +
                "def stream OutputStream; " +
                "" +
                "from StockStream " +
                "filter symbol == 'WSO2' and (price > 50 or volume == 25) " +
                "select symbol, price " +
                "insert into OutputStream;";

        WisdomApp wisdomApp = WisdomCompiler.parse(query);

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream",
                map("symbol", "WSO2", "price", 10.0),
                map("symbol", "WSO2", "price", 60.0));

        wisdomApp.start();

        InputHandler stockStreamInputHandler = wisdomApp.getInputHandler("StockStream");
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "WSO2", "price", 10.0, "volume", 25.0));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "WSO2", "price", 50.0, "volume", 10.0));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "WSO2", "price", 60.0, "volume", 15.0));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "ORACLE", "price", 20.0, "volume", 25.0));

        wisdomApp.shutdown();

        Assert.assertEquals("Incorrect number of events", 2, callback.getEventCount());
    }

    @Test
    public void testWindowQuery() {

        LOGGER.info("Test logical operator precedence query");

        String query = "@app(name='WisdomApp', version='1.0.0') " +
                "def stream StockStream; " +
                "def stream OutputStream; " +
                "" +
                "from StockStream " +
                "window.length(3) " +
                "select symbol, price " +
                "insert into OutputStream;";

        WisdomApp wisdomApp = WisdomCompiler.parse(query);

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream",
                map("symbol", "WSO2", "price", 10.0),
                map("symbol", "WSO2", "price", 50.0),
                map("symbol", "WSO2", "price", 60.0));

        wisdomApp.start();

        InputHandler stockStreamInputHandler = wisdomApp.getInputHandler("StockStream");
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "WSO2", "price", 10.0, "volume", 25.0));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "WSO2", "price", 50.0, "volume", 10.0));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "WSO2", "price", 60.0, "volume", 15.0));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "ORACLE", "price", 20.0, "volume", 25.0));

        wisdomApp.shutdown();

        Assert.assertEquals("Incorrect number of events", 3, callback.getEventCount());
    }
}
