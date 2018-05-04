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

public class TestPcapFileSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestPcapFileSource.class);

    static {
        ImportsManager.INSTANCE.use(PcapFileSource.class);
    }

    @Test
    public void testPcapFileSource1() throws InterruptedException, IOException {
        LOGGER.info("Test PcapFileSource with valid path");

        String pcapFile = TestPcapFileSource.class.getClassLoader().getResource("icmp.pcap").getPath();
        System.out.println(pcapFile);
        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("PacketStream");
        wisdomApp.defineStream("OutputStream");

        wisdomApp.defineQuery("query1")
                .from("PacketStream")
                .select("src_ip", "dst_ip")
                .insertInto("OutputStream");
        wisdomApp.addSource("PacketStream", Source.create("file.pcap", map("path", pcapFile)));

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, wisdomApp, "OutputStream",
                map("src_ip", "2.1.1.1", "dst_ip", "2.1.1.2"));

        wisdomApp.start();


        Thread.sleep(100);

        wisdomApp.shutdown();


        Assert.assertEquals("Incorrect number of events", 1, callback.getEventCount());
    }


    @Test(expected = WisdomAppValidationException.class)
    public void testPcapFileSource2() {
        LOGGER.info("Test PcapFileSource without path");

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("PacketStream");
        wisdomApp.defineStream("OutputStream");

        wisdomApp.defineQuery("query1")
                .from("PacketStream")
                .select("src_ip", "dst_ip")
                .insertInto("OutputStream");
        wisdomApp.addSource("StockStream", Source.create("file.pcap", Collections.emptyMap()));
    }
}
