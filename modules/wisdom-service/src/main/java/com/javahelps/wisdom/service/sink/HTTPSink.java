package com.javahelps.wisdom.service.sink;

import com.google.gson.ExclusionStrategy;
import com.google.gson.FieldAttributes;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.stream.Stream;
import com.javahelps.wisdom.core.stream.output.Sink;
import com.javahelps.wisdom.service.exception.WisdomServiceException;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;

import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.lang.reflect.Modifier;
import java.util.List;

public class HTTPSink implements Sink {

    private final String endpoint;
    private final Gson gson;
    private final HttpClient client = HttpClientBuilder.create().build();

    public HTTPSink(String endpoint) {
        this.endpoint = endpoint;
        this.gson = new GsonBuilder()
                .setExclusionStrategies(new ExclusionStrategy() {

                    public boolean shouldSkipClass(Class<?> clazz) {
                        if (Stream.class == clazz) {
                            return true;
                        }
                        return false;
                    }

                    public boolean shouldSkipField(FieldAttributes f) {
                        boolean skip = f.hasModifier(Modifier.TRANSIENT);
                        return skip;
                    }

                })
                .create();
    }

    @Override
    public void start() {

    }

    @Override
    public void publish(List<Event> events) throws IOException {

        HttpPost post = new HttpPost(endpoint);

        StringEntity input = new StringEntity(gson.toJson(events));
        input.setContentType(MediaType.APPLICATION_JSON);

        post.setEntity(input);

        HttpResponse response = client.execute(post);
        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode >= 400) {
            throw new WisdomServiceException(
                    String.format("Error in sending event %s to the endpoint %s. Response: %d, %s",
                            events.toString(), this.endpoint, statusCode, response.getStatusLine().getReasonPhrase()));
        }
    }

    @Override
    public void stop() {

    }
}
