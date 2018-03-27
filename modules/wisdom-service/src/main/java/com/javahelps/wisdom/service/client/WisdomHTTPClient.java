package com.javahelps.wisdom.service.client;

import com.javahelps.wisdom.service.Utility;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.javahelps.wisdom.service.Constant.MEDIA_APPLICATION_JSON;

public class WisdomHTTPClient extends WisdomClient {

    protected final String endpoint;
    protected final CloseableHttpClient client;

    public WisdomHTTPClient(String host, int port) {
        this.endpoint = String.format("http://%s:%d/WisdomApp/", host, port);
        this.client = HttpClientBuilder.create().setConnectionTimeToLive(0, TimeUnit.MILLISECONDS).build();
    }

    @Override
    public WisdomClient.Response send(String streamId, Map<String, Comparable> data) throws IOException {

        HttpPost post = new HttpPost(this.endpoint + streamId);
        StringEntity input = new StringEntity(Utility.toJson(data));
        input.setContentType(MEDIA_APPLICATION_JSON);
        post.setEntity(input);

        CloseableHttpResponse httpResponse = null;
        WisdomClient.Response response;
        try {
            httpResponse = client.execute(post);
            StatusLine statusLine = httpResponse.getStatusLine();
            response = new WisdomClient.Response(statusLine.getStatusCode(), statusLine.getReasonPhrase());
        } finally {
            if (httpResponse != null) {
                httpResponse.close();
            }
        }
        return response;
    }

    @Override
    public void close() throws IOException {
        this.client.close();
    }
}
