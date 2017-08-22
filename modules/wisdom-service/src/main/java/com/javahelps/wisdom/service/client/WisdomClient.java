package com.javahelps.wisdom.service.client;

import com.google.gson.Gson;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.Map;

public class WisdomClient {

    private final String endpoint;
    private final CloseableHttpClient client;
    private final Gson gson = new Gson();

    public WisdomClient(String host, int port) {
        this.endpoint = String.format("http://%s:%d/WisdomApp/", host, port);
        this.client = HttpClientBuilder.create().build();
    }

    public Response send(String streamId, Map<String, Comparable> data) throws IOException {

        HttpPost post = new HttpPost(this.endpoint + streamId);
        StringEntity input = new StringEntity(gson.toJson(data));
        input.setContentType(MediaType.APPLICATION_JSON);
        post.setEntity(input);

        CloseableHttpResponse httpResponse = client.execute(post);
        StatusLine statusLine = httpResponse.getStatusLine();
        Response response = new Response(statusLine.getStatusCode(), statusLine.getReasonPhrase());
        httpResponse.close();
        return response;
    }

    public void close() throws IOException {
        this.client.close();
    }

    public class Response {
        private int status;
        private String reason;

        private Response(int status, String reason) {
            this.status = status;
            this.reason = reason;
        }

        public int getStatus() {
            return status;
        }

        public String getReason() {
            return reason;
        }
    }
}
