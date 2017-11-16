package com.javahelps.wisdom.extensions.ml.quantizer;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.stream.InputHandler;
import com.javahelps.wisdom.core.util.EventGenerator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

import static com.javahelps.wisdom.extensions.ml.quantizer.TestUtil.map;

public class PredicateQuantizerTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(PredicateQuantizerTest.class);
    private AtomicInteger eventCount = new AtomicInteger(0);
    private TestUtil.CallbackUtil callbackUtil = new TestUtil.CallbackUtil(LOGGER, eventCount);

    @Before
    public void init() {
        this.eventCount.set(0);
    }

    @Test
    public void testQuantizer1() throws InterruptedException {
        LOGGER.info("Test quantizer 1 - OUT 5");

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("EventStream");
        wisdomApp.defineStream("OutputStream");

        wisdomApp.defineQuery("query1")
                .from("EventStream")
                .map(Quantizer.predicate("port", 0).greaterThan(1024, 1024))
                .insertInto("OutputStream");

        callbackUtil.addCallback(wisdomApp,
                map("port", 0),
                map("port", 80),
                map("port", 1023),
                map("port", 1024),
                map("port", 1024));

        wisdomApp.start();

        InputHandler loginEventStream = wisdomApp.getInputHandler("EventStream");
        loginEventStream.send(EventGenerator.generate("port", null));
        loginEventStream.send(EventGenerator.generate("port", 80));
        loginEventStream.send(EventGenerator.generate("port", 1023));
        loginEventStream.send(EventGenerator.generate("port", 1024));
        loginEventStream.send(EventGenerator.generate("port", 2048));

        Thread.sleep(100);

        wisdomApp.shutdown();

        Assert.assertEquals("Incorrect number of events", 5, eventCount.get());
    }
}
