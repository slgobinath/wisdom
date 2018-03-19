package com.javahelps.wisdom.service.client;

import org.apache.http.NoHttpResponseException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class WisdomAdminClient {

    private final String endpoint;
    private final CloseableHttpClient client;

    public WisdomAdminClient(String host, int port) {
        this.endpoint = String.format("http://%s:%d/WisdomApp/admin/", host, port);
        this.client = HttpClientBuilder.create().setConnectionTimeToLive(0, TimeUnit.MILLISECONDS).build();
    }

    public void stop() throws IOException {

        try {
            HttpPost post = new HttpPost(this.endpoint + "shutdown");
            CloseableHttpResponse httpResponse = client.execute(post);
            httpResponse.close();
        } catch (NoHttpResponseException ex) {
            // Do nothing
        } finally {
            this.close();
        }
    }

    public void close() throws IOException {
        this.client.close();
    }
}
