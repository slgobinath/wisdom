package com.javahelps.wisdom.extensions.ml.onehot;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.extension.ImportsManager;
import com.javahelps.wisdom.core.map.Mapper;
import com.javahelps.wisdom.core.operand.WisdomArray;
import com.javahelps.wisdom.core.stream.InputHandler;
import com.javahelps.wisdom.core.util.EventGenerator;
import com.javahelps.wisdom.extensions.ml.TestUtil;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static com.javahelps.wisdom.dev.util.Utility.map;

public class ToOneHotMapperTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ToOneHotMapperTest.class);

    static {
        ImportsManager.INSTANCE.use(ToOneHotMapper.class);
    }

    @Test
    public void testToOneHotMapper1() throws InterruptedException {
        LOGGER.info("Test ToOneHot mapper 1 - OUT 5");

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("StockStream");
        wisdomApp.defineStream("OutputStream");

        wisdomApp.defineQuery("query1")
                .from("StockStream")
                .map(Mapper.create("toOneHot", "symbol", Map.of("attr", "symbol", "items", WisdomArray.of("IBM", "WSO2", "ORACLE"))))
                .insertInto("OutputStream");

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream",
                map("symbol", WisdomArray.of(1, 0, 0), "price", 15.0),
                map("symbol", WisdomArray.of(0, 0, 1), "price", 25.0));

        wisdomApp.start();

        InputHandler inputHandler = wisdomApp.getInputHandler("StockStream");
        inputHandler.send(EventGenerator.generate("symbol", "IBM", "price", 15.0));
        inputHandler.send(EventGenerator.generate("symbol", "WSO2", "price", 25.0));

        Thread.sleep(100);

        wisdomApp.shutdown();

        Assert.assertEquals("Incorrect number of events", 2, callback.getEventCount());
    }
}
