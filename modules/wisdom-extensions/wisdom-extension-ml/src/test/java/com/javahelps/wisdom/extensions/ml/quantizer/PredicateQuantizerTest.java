package com.javahelps.wisdom.extensions.ml.quantizer;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.stream.InputHandler;
import com.javahelps.wisdom.core.util.EventGenerator;
import com.javahelps.wisdom.dev.test.TestCallback;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.javahelps.wisdom.dev.util.Utility.map;

public class PredicateQuantizerTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(PredicateQuantizerTest.class);
    private TestCallback callbackUtil = new TestCallback(LOGGER);

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

        TestCallback.TestResult testResult = callbackUtil.addCallback(wisdomApp, "OutputStream",
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

        testResult.assertTestResult(5);
    }
}
