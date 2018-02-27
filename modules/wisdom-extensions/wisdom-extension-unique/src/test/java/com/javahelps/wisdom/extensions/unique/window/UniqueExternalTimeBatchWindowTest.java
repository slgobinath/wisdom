package com.javahelps.wisdom.extensions.unique.window;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.operator.Operator;
import com.javahelps.wisdom.core.stream.InputHandler;
import com.javahelps.wisdom.core.util.EventGenerator;
import com.javahelps.wisdom.dev.test.TestCallback;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

import static com.javahelps.wisdom.dev.util.EventUtil.map;

public class UniqueExternalTimeBatchWindowTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(UniqueExternalTimeBatchWindowTest.class);
    private TestCallback callbackUtil = new TestCallback(LOGGER);

    @Test
    public void testWindow1() throws InterruptedException {
        LOGGER.info("Test window 1 - OUT 3");

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("LoginEventStream");
        wisdomApp.defineStream("OutputStream");

        wisdomApp.defineQuery("query1")
                .from("LoginEventStream")
                .window(UniqueWindow.externalTimeBatch("ip", "timestamp", Duration.of(1, ChronoUnit.SECONDS)))
                .aggregate(Operator.COUNT(), "count")
                .select("ip", "timestamp", "count")
                .insertInto("OutputStream");

        TestCallback.TestResult testResult = callbackUtil.addCallback(wisdomApp, "OutputStream",
                map("ip", "192.10.1.4", "timestamp", 1366335804342L, "count", 2L),
                map("ip", "192.10.1.4", "timestamp", 1366335805341L, "count", 1L),
                map("ip", "192.10.1.6", "timestamp", 1366335814345L, "count", 2L));

        wisdomApp.start();

        InputHandler loginEventStream = wisdomApp.getInputHandler("LoginEventStream");
        loginEventStream.send(EventGenerator.generate("ip", "192.10.1.3", "timestamp", 1366335804341L));
        loginEventStream.send(EventGenerator.generate("ip", "192.10.1.4", "timestamp", 1366335804342L));
        loginEventStream.send(EventGenerator.generate("ip", "192.10.1.4", "timestamp", 1366335805341L));
        loginEventStream.send(EventGenerator.generate("ip", "192.10.1.5", "timestamp", 1366335814341L));
        loginEventStream.send(EventGenerator.generate("ip", "192.10.1.6", "timestamp", 1366335814345L));
        loginEventStream.send(EventGenerator.generate("ip", "192.10.1.7", "timestamp", 1366335824341L));

        Thread.sleep(100);

        wisdomApp.shutdown();

        testResult.assertTestResult(3);
    }

    @Test
    public void testWindow2() throws InterruptedException {
        LOGGER.info("Test window 2 - OUT 2");

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("LoginEventStream");
        wisdomApp.defineStream("OutputStream");

        wisdomApp.defineQuery("query1")
                .from("LoginEventStream")
                .window(UniqueWindow.externalTimeBatch("ip", "timestamp", Duration.of(1, ChronoUnit.SECONDS)))
                .aggregate(Operator.COUNT(), "count")
                .select("ip", "timestamp", "count")
                .insertInto("OutputStream");

        TestCallback.TestResult testResult = callbackUtil.addCallback(wisdomApp, "OutputStream",
                map("ip", "192.10.1.4", "timestamp", 1366335805340L, "count", 2L),
                map("ip", "192.10.1.6", "timestamp", 1366335814545L, "count", 2L));

        wisdomApp.start();

        InputHandler loginEventStream = wisdomApp.getInputHandler("LoginEventStream");
        loginEventStream.send(EventGenerator.generate("ip", "192.10.1.3", "timestamp", 1366335804341L));
        loginEventStream.send(EventGenerator.generate("ip", "192.10.1.4", "timestamp", 1366335804342L));
        loginEventStream.send(EventGenerator.generate("ip", "192.10.1.4", "timestamp", 1366335805340L));
        loginEventStream.send(EventGenerator.generate("ip", "192.10.1.5", "timestamp", 1366335814341L));
        loginEventStream.send(EventGenerator.generate("ip", "192.10.1.5", "timestamp", 1366335814741L));
        loginEventStream.send(EventGenerator.generate("ip", "192.10.1.5", "timestamp", 1366335814641L));
        loginEventStream.send(EventGenerator.generate("ip", "192.10.1.6", "timestamp", 1366335814545L));
        loginEventStream.send(EventGenerator.generate("ip", "192.10.1.7", "timestamp", 1366335824341L));

        Thread.sleep(100);

        wisdomApp.shutdown();

        testResult.assertTestResult(2);
    }
}
