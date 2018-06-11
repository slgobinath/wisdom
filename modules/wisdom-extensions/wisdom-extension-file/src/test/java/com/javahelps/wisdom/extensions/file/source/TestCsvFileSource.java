package com.javahelps.wisdom.extensions.file.source;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.exception.WisdomAppValidationException;
import com.javahelps.wisdom.core.extension.ImportsManager;
import com.javahelps.wisdom.core.stream.input.Source;
import com.javahelps.wisdom.extensions.file.util.TestUtil;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;

import static com.javahelps.wisdom.core.util.Commons.map;

public class TestCsvFileSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestCsvFileSource.class);

    static {
        ImportsManager.INSTANCE.use(CsvFileSource.class);
    }

    @Test
    public void testCsvFileSource1() throws InterruptedException, IOException {
        LOGGER.info("Test CsvFileSource with valid path");

        String csvFile = TestCsvFileSource.class.getClassLoader().getResource("packets.csv").getPath();
        System.out.println(csvFile);
        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("PacketStream");
        wisdomApp.defineStream("OutputStream");

        wisdomApp.defineQuery("query1")
                .from("PacketStream")
                .insertInto("OutputStream");
        wisdomApp.addSource("PacketStream", Source.create("file.csv", map("path", csvFile, "highest_layer", "string", "timestamp", "long", "data", "string")));

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream",
                map("highest_layer", "TCP", "timestamp", 1499082958598L, "data", ""),
                map("highest_layer", "TCP", "timestamp", 1499082958598L, "data", ""),
                map("highest_layer", "TCP", "timestamp", 1499082958598L, "data", ""));

        wisdomApp.start();


        Thread.sleep(100);

        wisdomApp.shutdown();


        Assert.assertEquals("Incorrect number of events", 3, callback.getEventCount());
    }


    @Test(expected = WisdomAppValidationException.class)
    public void testCsvFileSource2() {
        LOGGER.info("Test CsvFileSource without path");

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("PacketStream");
        wisdomApp.defineStream("OutputStream");

        wisdomApp.defineQuery("query1")
                .from("PacketStream")
                .select("src_ip", "dst_ip")
                .insertInto("OutputStream");
        wisdomApp.addSource("StockStream", Source.create("file.csv", Collections.emptyMap()));
    }
}
