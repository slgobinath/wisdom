package com.javahelps.wisdom.service.source;

import com.google.gson.Gson;
import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.extension.ImportsManager;
import com.javahelps.wisdom.core.stream.input.Source;
import com.javahelps.wisdom.service.TestWisdomService;
import com.javahelps.wisdom.service.gprc.WisdomGrpc;
import com.javahelps.wisdom.service.gprc.WisdomGrpcService;
import com.javahelps.wisdom.service.util.TestUtil;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;

import static com.javahelps.wisdom.core.util.Commons.map;

public class GrpcSourceTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestWisdomService.class);

    static {
        ImportsManager.INSTANCE.use(GrpcSource.class);
    }

    @Test
    public void testGrpcSource1() throws InterruptedException {
        LOGGER.info("Test Grpc source 1 - OUT 3");

        WisdomApp app = new WisdomApp();
        app.defineStream("StockStream");
        app.defineStream("OutputStream");

        app.defineQuery("query1")
                .from("StockStream")
                .insertInto("OutputStream");

        app.addSource("StockStream", Source.create("grpc", map("port", 8080)));

        TestUtil.TestCallback callback = TestUtil.addStreamCallback(LOGGER, app, "OutputStream",
                map("symbol", "IBM", "price", 50.0, "volume", 10.0),
                map("symbol", "WSO2", "price", 60.0, "volume", 15.0),
                map("symbol", "ORACLE", "price", 70.0, "volume", 20.0));

        app.start();

        Thread.sleep(1000);

        Gson gson = new Gson();
        // Channel is the abstraction to connect to a service endpoint
        // Let's use plaintext communication because we don't have certs
        final ManagedChannel channel = ManagedChannelBuilder.forTarget("localhost:8080")
                .usePlaintext(true)
                .build();

        // It is up to the client to determine whether to block the call
        // Here we create a blocking stub, but an async stub,
        // or an async stub with Future are always possible.
        WisdomGrpc.WisdomStub stub = WisdomGrpc.newStub(channel);
        StreamObserver<WisdomGrpcService.Event> observer = stub.feed(new StreamObserver<>() {
            @Override
            public void onNext(WisdomGrpcService.Event event) {
                LOGGER.info("Received {}", event.getData());
            }

            @Override
            public void onError(Throwable throwable) {
                throwable.printStackTrace();
            }

            @Override
            public void onCompleted() {
                System.out.println("Completed");
            }
        });

        Map[] input = {map("symbol", "IBM", "price", 50.0, "volume", 10),
                map("symbol", "WSO2", "price", 60.0, "volume", 15),
                map("symbol", "ORACLE", "price", 70.0, "volume", 20)};

        Arrays.stream(input).map(gson::toJson).map(x -> WisdomGrpcService.Event.newBuilder()
                .setData(x)
                .build()).forEach(observer::onNext);
        observer.onCompleted();

        Thread.sleep(1000);

        // A Channel should be shutdown before stopping the process.
        channel.shutdown();

        Thread.sleep(1000);

        app.shutdown();

        Thread.sleep(100);

        Assert.assertEquals("Incorrect number of events", 3, callback.getEventCount());
    }
}
