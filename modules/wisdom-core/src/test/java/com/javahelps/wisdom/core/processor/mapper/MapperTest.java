package com.javahelps.wisdom.core.processor.mapper;

import com.javahelps.wisdom.core.TestUtil;
import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.map.Mapper;
import com.javahelps.wisdom.core.processor.partition.PartitionTestCase;
import com.javahelps.wisdom.core.stream.InputHandler;
import com.javahelps.wisdom.core.util.EventGenerator;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.javahelps.wisdom.core.util.Commons.map;

public class MapperTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(PartitionTestCase.class);

    @Test
    public void testMapName() throws InterruptedException {
        LOGGER.info("Test mapper 1 - OUT 2");

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("StockStream");
        wisdomApp.defineStream("OutputStream");

        wisdomApp.defineQuery("query1")
                .from("StockStream")
                .map(Mapper.rename("symbol", "name"), Mapper.rename("price", "cost"))
                .select("name", "cost")
                .insertInto("OutputStream");

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream",
                map("name", "IBM", "cost", 50.0),
                map("name", "ORACLE", "cost", 70.0));

        wisdomApp.start();

        InputHandler stockStreamInputHandler = wisdomApp.getInputHandler("StockStream");
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "ORACLE", "price", 70.0, "volume", 20));

        Thread.sleep(100);

        wisdomApp.shutdown();

        Assert.assertEquals("Incorrect number of events", 2, callback.getEventCount());
    }

    @Test
    public void testMapTimestamp() throws InterruptedException {
        LOGGER.info("Test mapper 2 - OUT 1");

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("StockStream");
        wisdomApp.defineStream("OutputStream");

        long timestamp = 1524789758000L;

        wisdomApp.defineQuery("query1")
                .from("StockStream")
                .map(Mapper.formatTime("timestamp", "time"))
                .select("name", "time")
                .insertInto("OutputStream");

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream",
                map("name", "Temp", "time", "2018-04-26T20:42:38"),
                map("name", "Temp", "time", "2018-04-26T20:43:38"));

        wisdomApp.start();

        InputHandler stockStreamInputHandler = wisdomApp.getInputHandler("StockStream");
        stockStreamInputHandler.send(EventGenerator.generate("name", "Temp", "timestamp", timestamp));
        stockStreamInputHandler.send(EventGenerator.generate("name", "Temp", "timestamp", timestamp + 60_000L));

        Thread.sleep(100);

        wisdomApp.shutdown();

        Assert.assertEquals("Incorrect number of events", 2, callback.getEventCount());
    }
}
