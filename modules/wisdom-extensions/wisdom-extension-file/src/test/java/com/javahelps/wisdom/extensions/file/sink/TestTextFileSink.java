package com.javahelps.wisdom.extensions.file.sink;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.exception.WisdomAppValidationException;
import com.javahelps.wisdom.core.extension.ImportsManager;
import com.javahelps.wisdom.core.stream.InputHandler;
import com.javahelps.wisdom.core.stream.output.Sink;
import com.javahelps.wisdom.core.util.EventGenerator;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;

import static com.javahelps.wisdom.core.util.Commons.map;

public class TestTextFileSink {

    static {
        ImportsManager.INSTANCE.use(TextFileSink.class);
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(TestTextFileSink.class);

    @BeforeClass
    public static void init() throws IOException, URISyntaxException {
        Files.deleteIfExists(Paths.get("output.log"));
    }

    @AfterClass
    public static void clean() throws IOException {
        Files.deleteIfExists(Paths.get("output.log"));
    }

    @Test
    public void testTextFileSink1() throws InterruptedException, IOException {
        LOGGER.info("Test TextFileSink with valid path");

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("StockStream");
        wisdomApp.defineStream("OutputStream");

        wisdomApp.defineQuery("query1")
                .from("StockStream")
                .select("symbol", "price")
                .insertInto("OutputStream");
        wisdomApp.addSink("OutputStream", Sink.create("file.text", map("path", "output.log")));


        wisdomApp.start();

        InputHandler stockStreamInputHandler = wisdomApp.getInputHandler("StockStream");
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "IBM", "price", 50.0, "volume", 10));
        stockStreamInputHandler.send(EventGenerator.generate("symbol", "WSO2", "price", 60.0, "volume", 15));

        Thread.sleep(100);

        wisdomApp.shutdown();

        String[] output = new String(Files.readAllBytes(Paths.get("output.log"))).split("\n");

        Assert.assertEquals("Incorrect number of events", 2, output.length);
        Assert.assertTrue("Event not found", output[0].contains("stream=OutputStream, data={symbol=IBM, price=50.0}, expired=false"));
        Assert.assertTrue("Event not found", output[1].contains("stream=OutputStream, data={symbol=WSO2, price=60.0}, expired=false"));
    }


    @Test(expected = WisdomAppValidationException.class)
    public void testTextFileSink2() {
        LOGGER.info("Test TextFileSink without path");

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("StockStream");
        wisdomApp.defineStream("OutputStream");

        wisdomApp.defineQuery("query1")
                .from("StockStream")
                .select("symbol", "price")
                .insertInto("OutputStream");
        wisdomApp.addSink("OutputStream", Sink.create("file.text", Collections.emptyMap()));
    }
}
