package com.javahelps.wisdom.extensions.ml.tf;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.extension.ImportsManager;
import com.javahelps.wisdom.core.map.Mapper;
import com.javahelps.wisdom.core.stream.InputHandler;
import com.javahelps.wisdom.core.util.EventGenerator;
import com.javahelps.wisdom.extensions.ml.TestUtil;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.javahelps.wisdom.core.util.Commons.map;
import static com.javahelps.wisdom.extensions.ml.util.Constants.*;

public class TensorFlowMapperTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(TensorFlowMapperTest.class);

    static {
        ImportsManager.INSTANCE.use(TensorFlowMapper.class);
    }

    @Test
    public void testTensorFlow1() throws InterruptedException {
        LOGGER.info("Test TensorFlow 1 - OUT 5");

        String modelPath = TensorFlowMapper.class.getClassLoader().getResource("tf_add_model").getPath();

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("EventStream");
        wisdomApp.defineStream("OutputStream");

        wisdomApp.defineQuery("query1")
                .from("EventStream")
                .select("x", "y")
                .map(Mapper.create("tensorFlow", "ans", map(PATH, modelPath, OPERATION, "ans", TYPE, "int")))
                .insertInto("OutputStream");

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream",
                map("x", 5, "y", 6, "ans", 11L),
                map("x", 10, "y", 20, "ans", 30L));

        wisdomApp.start();

        InputHandler inputHandler = wisdomApp.getInputHandler("EventStream");
        inputHandler.send(EventGenerator.generate("x", 5, "y", 6));
        inputHandler.send(EventGenerator.generate("x", 10, "y", 20));

        Thread.sleep(100);

        wisdomApp.shutdown();

        Assert.assertEquals("Incorrect number of events", 2, callback.getEventCount());
    }
}
