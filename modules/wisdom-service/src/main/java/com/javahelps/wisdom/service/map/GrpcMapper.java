package com.javahelps.wisdom.service.map;

import com.google.gson.Gson;
import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.exception.WisdomAppValidationException;
import com.javahelps.wisdom.core.extension.WisdomExtension;
import com.javahelps.wisdom.core.map.Mapper;
import com.javahelps.wisdom.core.util.Commons;
import com.javahelps.wisdom.service.gprc.WisdomGrpc;
import com.javahelps.wisdom.service.gprc.WisdomGrpcService;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.Map;

@WisdomExtension("grpc")
public class GrpcMapper extends Mapper {

    private final Gson gson = new Gson();
    private final String endpoint;
    private final String select;
    private ManagedChannel channel;
    private WisdomGrpc.WisdomBlockingStub stub;

    public GrpcMapper(String attrName, Map<String, ?> properties) {
        super(attrName, properties);
        this.endpoint = Commons.getProperty(properties, "endpoint", 0);
        this.select = Commons.getProperty(properties, "select", 1);
        if (this.endpoint == null) {
            throw new WisdomAppValidationException("Required property endpoint for gRpc mapper not found");
        }
        if (this.select == null) {
            throw new WisdomAppValidationException("Required property select for gRpc mapper not found");
        }
    }

    @Override
    public void start() {

    }

    @Override
    public synchronized void init(WisdomApp wisdomApp) {
        this.channel = ManagedChannelBuilder.forTarget(this.endpoint)
                .usePlaintext(true)
                .build();
        this.stub = WisdomGrpc.newBlockingStub(this.channel);
    }

    @Override
    public synchronized void stop() {
        if (this.channel != null) {
            this.channel.shutdown();
        }
    }

    @Override
    public Event map(Event event) {
        WisdomGrpcService.Event request =
                WisdomGrpcService.Event.newBuilder()
                        .setData(this.gson.toJson(event.getData()))
                        .build();

        WisdomGrpcService.Event response = this.stub.send(request);
        Map<String, Object> data = gson.fromJson(response.getData(), Map.class);
        event.set(this.attrName, data.get(this.select));
        return event;
    }
}
