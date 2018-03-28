package com.javahelps.wisdom.core.statistics;

import com.javahelps.wisdom.core.TestUtil;
import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.stream.InputHandler;
import com.javahelps.wisdom.core.util.EventGenerator;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestStreamTracker {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestStreamTracker.class);

    @Test
    public void testThroughput() throws InterruptedException {
        LOGGER.info("Test throughput - 4");

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("StockStream");
        wisdomApp.defineStream("OutputStream");
        wisdomApp.defineStream("StatisticsStream");
        wisdomApp.defineStream("FilteredStatisticsStream");

        wisdomApp.defineQuery("query1")
                .from("StockStream")
                .select("symbol", "price")
                .insertInto("OutputStream");

        wisdomApp.defineQuery("query2")
                .from("StatisticsStream")
                .select("name", "throughput")
                .insertInto("FilteredStatisticsStream");

        StatisticsManager manager = wisdomApp.enableStatistics("StatisticsStream", 1000L);
        wisdomApp.getStream("StockStream").setTracker(manager.createStreamTracker("StockStream"));
        wisdomApp.getStream("OutputStream").setTracker(manager.createStreamTracker("OutputStream"));

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
}
