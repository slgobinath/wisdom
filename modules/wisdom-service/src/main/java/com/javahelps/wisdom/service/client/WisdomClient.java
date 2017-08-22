package com.javahelps.wisdom.service.client;

import com.google.gson.Gson;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;

import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.Map;

public class WisdomClient {

    private final String endpoint;
    private final HttpClient client;
    private final Gson gson = new Gson();

    public WisdomClient(String host, int port) {
        this.endpoint = String.format("http://%s:%d/WisdomApp/", host, port);
        this.client = HttpClientBuilder.create().build();
    }

    public HttpResponse send(String streamId, Map<String, Comparable> data) throws IOException {

        HttpPost post = new HttpPost(this.endpoint + streamId);
        StringEntity input = new StringEntity(gson.toJson(data));
        input.setContentType(MediaType.APPLICATION_JSON);
        post.setEntity(input);

        return client.execute(post);
    }
}
