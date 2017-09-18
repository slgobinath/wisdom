package com.javahelps.wisdom.extensions.unique.window;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.stream.InputHandler;
import com.javahelps.wisdom.core.util.EventGenerator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;

import static com.javahelps.wisdom.extensions.unique.window.TestUtil.map;

public class PacketDataValidatorTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(PacketDataValidatorTest.class);
    private AtomicInteger eventCount = new AtomicInteger(0);
    private TestUtil.CallbackUtil callbackUtil = new TestUtil.CallbackUtil(LOGGER, eventCount);

    @Before
    public void init() {
        this.eventCount.set(0);
    }

    @Test
    public void testValidator1() throws InterruptedException {
        LOGGER.info("Test window 1 - OUT 3");

        // Pattern: |ABC|;|PQR|;within:10;
        Path patternPath = Paths.get(ClassLoader.getSystemClassLoader().getResource("pattern.txt").getPath());

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("EventStream");
        wisdomApp.defineStream("OutputStream");

        wisdomApp.defineQuery("query1")
                .from("EventStream")
                .filter(PacketDataValidator.construct("data", patternPath))
                .insertInto("OutputStream");

        callbackUtil.addCallback(wisdomApp, map("data", "ABCPQR"), map("data", "ABCxxPQR"));

        wisdomApp.start();

        InputHandler loginEventStream = wisdomApp.getInputHandler("EventStream");
        loginEventStream.send(EventGenerator.generate("data", "ABCDEF"));
        loginEventStream.send(EventGenerator.generate("data", "ABCPQR"));
        loginEventStream.send(EventGenerator.generate("data", "ABCxxPQR"));
        loginEventStream.send(EventGenerator.generate("data", "ABCxxxxxPQR"));

        Thread.sleep(100);

        wisdomApp.shutdown();

        Assert.assertEquals("Incorrect number of events", 2, eventCount.get());
    }
}
