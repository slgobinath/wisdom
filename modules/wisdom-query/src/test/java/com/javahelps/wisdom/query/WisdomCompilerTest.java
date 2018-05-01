package com.javahelps.wisdom.query;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.stream.InputHandler;
import com.javahelps.wisdom.core.util.EventGenerator;
import com.javahelps.wisdom.core.variable.Variable;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import static com.javahelps.wisdom.query.TestUtil.map;

public class WisdomCompilerTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(WisdomCompilerTest.class);

    @Test
    public void testSelectAttributesQuery() {

        LOGGER.info("Test select attributes query");

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
    public void testSelectEventsQuery() {

        LOGGER.info("Test select events query");

        String query = "@app(name='WisdomApp', version='1.0.0') " +
                "def stream StockStream; " +
                "def stream OutputStream; " +
                "" +
                "from StockStream " +
                "window.lengthBatch(3) " +
                "select -2, -1 " +
                "insert into OutputStream;";

        WisdomApp wisdomApp = WisdomCompiler.parse(query);

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream",
                map("symbol", "WSO2", "price", 60.0, "volume", 15),
                map("symbol", "GOOGLE", "price", 70.0, "volume", 20));

        wisdomApp.start();

        InputHandler stockStreamInputHandler = wisdomApp.getInputHandler("StockStream");
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "WSO2", "price", 60.0, "volume", 15));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "GOOGLE", "price", 70.0, "volume", 20));

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
    public void testGreaterThanOrEqualFilterQuery() {

        LOGGER.info("Test greater than filter query");

        String query = "@app(name='WisdomApp', version='1.0.0') " +
                "def stream StockStream; " +
                "def stream OutputStream; " +
                "def stream ThresholdStream; " +
                "def variable threshold = 20; " +
                "" +
                "from StockStream " +
                "filter volume >= $threshold " +
                "select symbol, price " +
                "insert into OutputStream; " +
                "" +
                "from ThresholdStream " +
                "update threshold;";

        WisdomApp wisdomApp = WisdomCompiler.parse(query);

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream",
                map("symbol", "WSO2", "price", 60.0),
                map("symbol", "ORACLE", "price", 70.0));

        wisdomApp.start();

        InputHandler stockStreamInputHandler = wisdomApp.getInputHandler("StockStream");
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10));
        wisdomApp.getInputHandler("ThresholdStream").send(EventGenerator.generate("threshold", 15));
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
    public void testLessThanOrEqualFilterQuery() {

        LOGGER.info("Test greater than filter query");

        String query = "@app(name='WisdomApp', version='1.0.0') " +
                "def stream StockStream; " +
                "def stream OutputStream; " +
                "def stream ThresholdStream; " +
                "def variable threshold = 5; " +
                "" +
                "from StockStream " +
                "filter 15 <= $threshold " +
                "select symbol, price " +
                "insert into OutputStream; " +
                "" +
                "from ThresholdStream " +
                "update threshold;";

        WisdomApp wisdomApp = WisdomCompiler.parse(query);

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream",
                map("symbol", "WSO2", "price", 60.0),
                map("symbol", "ORACLE", "price", 70.0));

        wisdomApp.start();

        InputHandler stockStreamInputHandler = wisdomApp.getInputHandler("StockStream");
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10));
        wisdomApp.getInputHandler("ThresholdStream").send(EventGenerator.generate("threshold", 15));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "WSO2", "price", 60.0, "volume", 15));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "ORACLE", "price", 70.0, "volume", 25));

        wisdomApp.shutdown();

        Assert.assertEquals("Incorrect number of events", 2, callback.getEventCount());
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
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "WSO2", "price", 10.0, "volume", 25));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "WSO2", "price", 50.0, "volume", 10));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "WSO2", "price", 60.0, "volume", 15));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "ORACLE", "price", 20.0, "volume", 25));

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
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "WSO2", "price", 10.0, "volume", 25));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "WSO2", "price", 50.0, "volume", 10));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "WSO2", "price", 60.0, "volume", 15));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "ORACLE", "price", 20.0, "volume", 25));

        wisdomApp.shutdown();

        Assert.assertEquals("Incorrect number of events", 2, callback.getEventCount());
    }

    @Test
    public void testLengthWindowQuery() {

        LOGGER.info("Test length window");

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
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "WSO2", "price", 10.0, "volume", 25));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "WSO2", "price", 50.0, "volume", 10));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "WSO2", "price", 60.0, "volume", 15));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "ORACLE", "price", 20.0, "volume", 25));

        wisdomApp.shutdown();

        Assert.assertEquals("Incorrect number of events", 3, callback.getEventCount());
    }

    @Test
    public void testExternalTimeBatchWindowQuery() throws InterruptedException {
        LOGGER.info("Test external time batch window");

        String query = "@app(name='WisdomApp', version='1.0.0') " +
                "def stream StockStream; " +
                "def stream OutputStream; " +
                "" +
                "from StockStream " +
                "window.externalTimeBatch('timestamp', time.second(1)) " +
                "select symbol, price " +
                "insert into OutputStream;";

        WisdomApp wisdomApp = WisdomCompiler.parse(query);

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream",
                map("symbol", "IBM", "price", 50.0),
                map("symbol", "WSO2", "price", 60.0));

        wisdomApp.start();

        InputHandler stockStreamInputHandler = wisdomApp.getInputHandler("StockStream");
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "IBM", "price", 50.0, "timestamp", 1000L));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "WSO2", "price", 60.0, "timestamp", 1500L));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "ORACLE", "price", 70.0, "timestamp", 2000L));

        Thread.sleep(100);

        Assert.assertEquals("Incorrect number of events", 2, callback.getEventCount());
    }

    @Test
    public void testAsyncApp() throws InterruptedException {

        LOGGER.info("Test Wisdom app async annotation");

        String query = "@app(name='WisdomApp', version='1.0.0', async=true, buffer=32) " +
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

        Thread.sleep(100);

        wisdomApp.shutdown();

        Assert.assertEquals("Incorrect number of events", 2, callback.getEventCount());
    }

    @Test
    public void testAsyncQuery() throws InterruptedException {

        LOGGER.info("Test stream async annotation");

        String query = "@app(name='WisdomApp', version='1.0.0') " +
                "@config(async=true, buffer=32) " +
                "def stream FilterStream; " +
                "def stream StockStream; " +
                "def stream OutputStream; " +
                "" +
                "from StockStream " +
                "filter volume > 10 " +
                "insert into FilterStream; " +
                "" +
                "from FilterStream " +
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

        Thread.sleep(100);

        wisdomApp.shutdown();

        Assert.assertEquals("Incorrect number of events", 2, callback.getEventCount());
    }

    @Test
    public void testVariableWithLengthBatchWindow() throws InterruptedException {
        LOGGER.info("Test window 4 - OUT 3");

        String query = "@app(name='WisdomApp', version='1.0.0') " +
                "def variable window_length = 3; " +
                "def stream StockStream; " +
                "def stream OutputStream; " +
                "def stream VariableStream; " +
                "" +
                "from StockStream " +
                "filter price > 55.0 " +
                "window.lengthBatch($window_length) " +
                "select symbol, price " +
                "insert into OutputStream; " +
                "" +
                "from VariableStream " +
                "filter window_length > 0 " +
                "update window_length; ";

        WisdomApp wisdomApp = WisdomCompiler.parse(query);

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream",
                map("symbol", "WSO2", "price", 60.0),
                map("symbol", "ORACLE", "price", 70.0));

        wisdomApp.start();

        InputHandler stockStreamInputHandler = wisdomApp.getInputHandler("StockStream");
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "WSO2", "price", 60.0, "volume", 15));
        // Update the window length
        wisdomApp.getInputHandler("VariableStream").send(EventGenerator.generate("window_length", 2));
        Thread.sleep(100);
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "ORACLE", "price", 70.0, "volume", 20));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "GOOGLE", "price", 80.0, "volume", 25));

        Thread.sleep(100);
        wisdomApp.shutdown();

        Assert.assertEquals("Incorrect number of events", 2, callback.getEventCount());
    }

    @Test
    public void testCompileFromFile() throws URISyntaxException, IOException {

        LOGGER.info("Test query file");

        WisdomApp wisdomApp = WisdomCompiler.parse(
                Paths.get(ClassLoader.getSystemClassLoader().getResource("wisdom_app.wisdomql").toURI()));

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
    public void testPartition() throws InterruptedException {
        LOGGER.info("Test partition - OUT 2");

        String query = "def stream StockStream; " +
                "def stream OutputStream; " +
                "" +
                "from StockStream " +
                "partition by symbol " +
                "window.lengthBatch(2) " +
                "aggregate sum(price) as price " +
                "select symbol, price " +
                "insert into OutputStream;";

        WisdomApp wisdomApp = WisdomCompiler.parse(query);

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream",
                map("symbol", "IBM", "price", 110.0),
                map("symbol", "ORACLE", "price", 150.0));

        wisdomApp.start();

        InputHandler stockStreamInputHandler = wisdomApp.getInputHandler("StockStream");
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "ORACLE", "price", 70.0, "volume", 20));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "IBM", "price", 60.0, "volume", 15));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "ORACLE", "price", 80.0, "volume", 25));

        Thread.sleep(100);

        Assert.assertEquals("Incorrect number of events", 2, callback.getEventCount());
    }

    @Test
    public void testConsoleSink() {

        LOGGER.info("Test console sink annotation");

        String query = "@app(name='WisdomApp', version='1.0.0') " +
                "def stream StockStream; " +
                "@sink(type='console')" +
                "@sink(type='console')" +
                "def stream OutputStream; " +
                "" +
                "from StockStream " +
                "select symbol, price " +
                "insert into OutputStream;";

        WisdomApp wisdomApp = WisdomCompiler.parse(query);

        wisdomApp.start();

        ByteArrayOutputStream bos = new ByteArrayOutputStream();

        PrintStream originalStream = System.out;
        System.setOut(new PrintStream(bos));

        InputHandler stockStreamInputHandler = wisdomApp.getInputHandler("StockStream");
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10));

        System.setOut(originalStream);

        wisdomApp.shutdown();

        Assert.assertTrue("Incorrect number of events", bos.toString().contains("{symbol=IBM, price=50.0}"));
    }

    @Test
    public void testTrainableVariable() {

        LOGGER.info("Test trainable annotation");

        String query = "@app(name='WisdomApp', version='1.0.0') " +
                "def stream StockStream; " +
                "def stream OutputStream; " +
                "def stream ThresholdStream; " +
                "@config(trainable=true) " +
                "def variable threshold = 20; " +
                "" +
                "from StockStream " +
                "filter volume >= $threshold " +
                "select symbol, price " +
                "insert into OutputStream; " +
                "" +
                "from ThresholdStream " +
                "update threshold;";

        WisdomApp wisdomApp = WisdomCompiler.parse(query);

        List<Variable> trainableVariables = wisdomApp.getTrainable();

        Assert.assertEquals("Incorrect number of trainable variables", 1, trainableVariables.size());
        Assert.assertEquals("Incorrect variable", "threshold", trainableVariables.get(0).getId());
    }

    @Test
    public void testStreamThroughput() throws InterruptedException {

        LOGGER.info("Test stream stats");

        String query = "@app(name='WisdomApp', version='1.0.0', stats='StatisticsStream', stats_freq=time.sec(1)) " +
                "@config(stats=true) " +
                "def stream StockStream; " +
                "@config(stats=true) " +
                "def stream OutputStream; " +
                "def stream StatisticsStream; " +
                "def stream FilteredStatisticsStream; " +
                "" +
                "from StockStream " +
                "select symbol, price " +
                "insert into OutputStream; " +
                "" +
                "from StatisticsStream " +
                "select name, throughput " +
                "insert into FilteredStatisticsStream; ";

        WisdomApp wisdomApp = WisdomCompiler.parse(query);

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "FilteredStatisticsStream",
                TestUtil.map("name", "StockStream", "throughput", 2.0),
                TestUtil.map("name", "OutputStream", "throughput", 2.0),
                TestUtil.map("name", "StockStream", "throughput", 0.0),
                TestUtil.map("name", "OutputStream", "throughput", 0.0));

        wisdomApp.start();

        InputHandler stockStreamInputHandler = wisdomApp.getInputHandler("StockStream");
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "WSO2", "price", 60.0, "volume", 15));

        Thread.sleep(2100);

        wisdomApp.shutdown();

        Assert.assertEquals("Incorrect number of events", 4, callback.getEventCount());
    }

    @Test
    public void testStatsVariableSelection() throws InterruptedException {

        LOGGER.info("Test stream stats");

        String query = "@app(name='WisdomApp', version='1.0.0', stats='StatisticsStream', stats_freq=time.sec(1), stats_vars=['port', 'version'], port=8080) " +
                "def stream StockStream; " +
                "@config(stats=true) " +
                "def stream OutputStream; " +
                "def stream StatisticsStream; " +
                "def stream FilteredStatisticsStream; " +
                "" +
                "from StockStream " +
                "select symbol, price " +
                "insert into OutputStream; " +
                "" +
                "from StatisticsStream " +
                "select app, port, name, throughput " +
                "insert into FilteredStatisticsStream; ";

        WisdomApp wisdomApp = WisdomCompiler.parse(query);

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "FilteredStatisticsStream",
                TestUtil.map("app", "WisdomApp", "name", "OutputStream", "throughput", 2.0, "port", 8080L),
                TestUtil.map("app", "WisdomApp", "name", "OutputStream", "throughput", 0.0, "port", 8080L));

        wisdomApp.start();

        InputHandler stockStreamInputHandler = wisdomApp.getInputHandler("StockStream");
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "WSO2", "price", 60.0, "volume", 15));

        Thread.sleep(2200);

        wisdomApp.shutdown();

        Assert.assertEquals("Incorrect number of events", 2, callback.getEventCount());
    }

    @Test
    public void testCommentInQuery() {

        LOGGER.info("Test comment in query");

        String query = "/*multiline comment*/ @app(name='WisdomApp', version='1.0.0') /*\n " +
                "hello world \n" +
                "end of comment*/ " +
                "# this is a comment\n " +
                "def stream StockStream; # another comment\n " +
                "def stream OutputStream; " +
                "" +
                "from StockStream " +
                "select symbol, price " +
                "insert into OutputStream;";

        WisdomCompiler.parse(query);
    }

    @Test
    public void testStringInQuery() {

        LOGGER.info("Test 'val' IN attribute query");

        String query = "@app(name='WisdomApp', version='1.0.0') " +
                "def stream StockStream; " +
                "def stream OutputStream; " +
                "" +
                "from StockStream " +
                "filter 'O' in symbol " +
                "select symbol, price " +
                "insert into OutputStream;";

        WisdomApp wisdomApp = WisdomCompiler.parse(query);

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream",
                map("symbol", "WSO2", "price", 60.0),
                map("symbol", "AMAZON", "price", 70.0));

        wisdomApp.start();

        InputHandler stockStreamInputHandler = wisdomApp.getInputHandler("StockStream");
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "WSO2", "price", 60.0, "volume", 15));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "AMAZON", "price", 70.0, "volume", 20));

        wisdomApp.shutdown();

        Assert.assertEquals("Incorrect number of events", 2, callback.getEventCount());
    }

    @Test
    public void testRegexInQuery() {

        LOGGER.info("Test '\\d+' IN attribute query");

        String query = "@app(name='WisdomApp', version='1.0.0') " +
                "def stream PacketStream; " +
                "def stream OutputStream; " +
                "" +
                "from PacketStream " +
                "filter 'Keep-Alive: \\\\d+' in header " +
                "select ip, port " +
                "insert into OutputStream;";

        WisdomApp wisdomApp = WisdomCompiler.parse(query);

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream",
                map("ip", "127.0.0.1", "port", 80),
                map("ip", "127.0.0.2", "port", 80));

        wisdomApp.start();

        InputHandler stockStreamInputHandler = wisdomApp.getInputHandler("PacketStream");
        stockStreamInputHandler.send(EventGenerator.generate("ip", "127.0.0.1", "port", 80, "header", "Keep-Alive: 768"));
        stockStreamInputHandler.send(EventGenerator.generate("ip", "127.0.0.2", "port", 80, "header", "Keep-Alive: 985"));
        stockStreamInputHandler.send(EventGenerator.generate("ip", "127.0.0.3", "port", 80, "header", "Connection: Keep-Alive"));

        wisdomApp.shutdown();

        Assert.assertEquals("Incorrect number of events", 2, callback.getEventCount());
    }

    @Test
    public void testNewLineInQuery() {

        LOGGER.info("Test new line character IN attribute query");

        String query = "@app(name='WisdomApp', version='1.0.0') " +
                "def stream StockStream; " +
                "def stream OutputStream; " +
                "" +
                "from StockStream " +
                "filter '\\r\\n' in data " +
                "select symbol, price " +
                "insert into OutputStream;";

        WisdomApp wisdomApp = WisdomCompiler.parse(query);

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream",
                map("symbol", "WSO2", "price", 60.0),
                map("symbol", "AMAZON", "price", 70.0));

        wisdomApp.start();

        InputHandler stockStreamInputHandler = wisdomApp.getInputHandler("StockStream");
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "IBM", "price", 50.0, "data", "hello world"));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "WSO2", "price", 60.0, "data", "hello\r\nworld"));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "AMAZON", "price", 70.0, "data", "hello world\r\n"));

        wisdomApp.shutdown();

        Assert.assertEquals("Incorrect number of events", 2, callback.getEventCount());
    }

    @Test
    public void testRenameAttributesQuery() {

        LOGGER.info("Test rename attributes query");

        String query = "@app(name='WisdomApp', version='1.0.0') " +
                "def stream StockStream; " +
                "def stream OutputStream; " +
                "" +
                "from StockStream " +
                "map symbol as name, price as cost " +
                "select name, cost " +
                "insert into OutputStream;";

        WisdomApp wisdomApp = WisdomCompiler.parse(query);

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream",
                map("name", "IBM", "cost", 50.0),
                map("name", "WSO2", "cost", 60.0));

        wisdomApp.start();

        InputHandler stockStreamInputHandler = wisdomApp.getInputHandler("StockStream");
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "WSO2", "price", 60.0, "volume", 15));

        wisdomApp.shutdown();

        Assert.assertEquals("Incorrect number of events", 2, callback.getEventCount());
    }

    @Test
    public void testFormatTimeMapperQuery() {

        LOGGER.info("Test rename attributes query");

        long timestamp = 1524789758000L;

        String query = "@app(name='WisdomApp', version='1.0.0') " +
                "def stream StockStream; " +
                "def stream OutputStream; " +
                "" +
                "from StockStream " +
                "map formatTime('timestamp', 'UTC') as dateTime, dateTime as newTime " +
                "select name, newTime " +
                "insert into OutputStream;";

        WisdomApp wisdomApp = WisdomCompiler.parse(query);

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream",
                map("name", "Temp", "newTime", "2018-04-27T00:42:38"),
                map("name", "Temp", "newTime", "2018-04-27T00:43:38"));

        wisdomApp.start();

        InputHandler stockStreamInputHandler = wisdomApp.getInputHandler("StockStream");
        stockStreamInputHandler.send(EventGenerator.generate("name", "Temp", "timestamp", timestamp));
        stockStreamInputHandler.send(EventGenerator.generate("name", "Temp", "timestamp", timestamp + 60_000L));

        wisdomApp.shutdown();

        Assert.assertEquals("Incorrect number of events", 2, callback.getEventCount());
    }

    @Test
    public void testConstantMapperQuery() {

        LOGGER.info("Test constant as query");

        String query = "@app(name='WisdomApp', version='1.0.0') " +
                "def stream StockStream; " +
                "def stream OutputStream; " +
                "" +
                "from StockStream " +
                "map true as released, 50 as volume, time.sec(1) as timestamp " +
                "insert into OutputStream;";

        WisdomApp wisdomApp = WisdomCompiler.parse(query);

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream",
                map("symbol", "IBM", "price", 50.0, "volume", 50L, "released", true, "timestamp", 1000L),
                map("symbol", "WSO2", "price", 60.0, "volume", 50L, "released", true, "timestamp", 1000L));

        wisdomApp.start();

        InputHandler stockStreamInputHandler = wisdomApp.getInputHandler("StockStream");
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "WSO2", "price", 60.0, "volume", 15));

        wisdomApp.shutdown();

        Assert.assertEquals("Incorrect number of events", 2, callback.getEventCount());
    }

    @Test
    public void testVariableMapperQuery() {

        LOGGER.info("Test variable as query");

        String query = "@app(name='WisdomApp', version='1.0.0') " +
                "def stream StockStream; " +
                "def stream OutputStream; " +
                "def variable released = false; " +
                "" +
                "from StockStream " +
                "map $released as released " +
                "insert into OutputStream;";

        WisdomApp wisdomApp = WisdomCompiler.parse(query);

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream",
                map("symbol", "IBM", "price", 50.0, "volume", 10L, "released", false),
                map("symbol", "WSO2", "price", 60.0, "volume", 15L, "released", true));

        wisdomApp.start();

        InputHandler stockStreamInputHandler = wisdomApp.getInputHandler("StockStream");
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10L));
        wisdomApp.getVariable("released").set(true);
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "WSO2", "price", 60.0, "volume", 15L));

        wisdomApp.shutdown();

        Assert.assertEquals("Incorrect number of events", 2, callback.getEventCount());
    }

    @Test
    public void testDataTypeMapperQuery() {

        LOGGER.info("Test int(x) as x query");

        String query = "@app(name='WisdomApp', version='1.0.0') " +
                "def stream StockStream; " +
                "def stream OutputStream; " +
                "" +
                "from StockStream " +
                "map int('price') as price, float('volume') as volume, bool('symbol') as symbol " +
                "insert into OutputStream;";

        WisdomApp wisdomApp = WisdomCompiler.parse(query);

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream",
                Map.of("symbol", true, "price", 50, "volume", 10.0f),
                Map.of("symbol", false, "price", 60, "volume", 15.0f));

        wisdomApp.start();

        InputHandler stockStreamInputHandler = wisdomApp.getInputHandler("StockStream");
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "IBM", "price", 50.1, "volume", 10L));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "", "price", 60.5, "volume", 15L));

        wisdomApp.shutdown();

        Assert.assertEquals("Incorrect number of events", 2, callback.getEventCount());
    }
}
