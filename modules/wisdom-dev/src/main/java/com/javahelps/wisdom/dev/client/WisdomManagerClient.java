package com.javahelps.wisdom.dev.client;

import com.google.gson.Gson;
import com.javahelps.wisdom.dev.util.Utility;
import com.javahelps.wisdom.query.WisdomCompiler;
import org.apache.http.NoHttpResponseException;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.*;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.javahelps.wisdom.core.util.WisdomConstants.THRESHOLD_STREAM;
import static com.javahelps.wisdom.dev.util.Constants.HTTP_OK;
import static com.javahelps.wisdom.dev.util.Constants.MEDIA_APPLICATION_JSON;
import static com.javahelps.wisdom.dev.util.Utility.map;

public class WisdomManagerClient implements AutoCloseable {

    protected final String endpoint;
    protected final CloseableHttpClient client;
    private final Gson gson = new Gson();

    public WisdomManagerClient(String host, int port) {
        this.endpoint = String.format("http://%s:%d/WisdomManager/", host, port);
        this.client = HttpClientBuilder.create().setConnectionTimeToLive(0, TimeUnit.MILLISECONDS).build();
    }

    public void shutdown() throws IOException {

        try {
            HttpPost post = new HttpPost(this.endpoint + "stop");
            CloseableHttpResponse httpResponse = this.client.execute(post);
            httpResponse.close();
        } catch (NoHttpResponseException ex) {
            // Do nothing
        } finally {
            this.close();
        }
    }

    public void start(String appName) throws IOException {

        try {
            HttpPost post = new HttpPost(this.endpoint + "start/" + appName);
            CloseableHttpResponse httpResponse = this.client.execute(post);
            httpResponse.close();
        } catch (NoHttpResponseException ex) {
            // Do nothing
        } finally {
            this.close();
        }
    }

    public void stop(String appName) throws IOException {

        try {
            HttpPost post = new HttpPost(this.endpoint + "stop/" + appName);
            CloseableHttpResponse httpResponse = this.client.execute(post);
            httpResponse.close();
        } catch (NoHttpResponseException ex) {
            // Do nothing
        } finally {
            this.close();
        }
    }

    public void delete(String appName) throws IOException {

        try {
            HttpDelete delete = new HttpDelete(this.endpoint + "app/" + appName);
            CloseableHttpResponse httpResponse = this.client.execute(delete);
            httpResponse.close();
        } catch (NoHttpResponseException ex) {
            // Do nothing
        } finally {
            this.close();
        }
    }

    public void initialize(String appName, Map<String, Comparable> variable) throws IOException {

        try {
            HttpPatch patch = new HttpPatch(this.endpoint + "app/" + appName);
            StringEntity input = new StringEntity(Utility.toJson(Map.of(THRESHOLD_STREAM, variable)));
            input.setContentType(MEDIA_APPLICATION_JSON);
            patch.setEntity(input);
            CloseableHttpResponse httpResponse = this.client.execute(patch);
            httpResponse.close();
        } catch (NoHttpResponseException ex) {
            // Do nothing
        } finally {
            this.close();
        }
    }


    public Response deploy(Path queryFile, int port) throws IOException {
        return this.deploy(new String(Files.readAllBytes(queryFile)), port);
    }

    public Response deploy(String query, int port) throws IOException {

        // Compile to validate query
        WisdomCompiler.parse(query);

        HttpPost post = new HttpPost(this.endpoint + "app");
        StringEntity input = new StringEntity(Utility.toJson(map("query", query, "port", port)));
        input.setContentType(MEDIA_APPLICATION_JSON);
        post.setEntity(input);

        CloseableHttpResponse httpResponse = null;
        Response response;
        try {
            httpResponse = client.execute(post);
            StatusLine statusLine = httpResponse.getStatusLine();
            response = new Response(statusLine.getStatusCode(), statusLine.getReasonPhrase());
        } finally {
            if (httpResponse != null) {
                httpResponse.close();
            }
        }
        return response;
    }

    public List<Map<String, Comparable>> info() throws IOException {

        List<Map<String, Comparable>> list = null;
        HttpGet get = new HttpGet(this.endpoint + "app");
        CloseableHttpResponse httpResponse = null;
        try {
            httpResponse = this.client.execute(get);
            if (httpResponse.getStatusLine().getStatusCode() == HTTP_OK) {
                String response;
                try (BufferedReader buffer = new BufferedReader(new InputStreamReader(httpResponse.getEntity().getContent()))) {
                    response = buffer.lines().collect(Collectors.joining("\n"));
                }
                list = this.gson.fromJson(response, List.class);
            }
        } finally {
            if (httpResponse != null) {
                httpResponse.close();
            }
        }
        return list;
    }

    public Map<String, Comparable> info(String appName) throws IOException {

        Map<String, Comparable> map = null;
        HttpGet get = new HttpGet(this.endpoint + "app/" + appName);
        CloseableHttpResponse httpResponse = null;
        try {
            httpResponse = this.client.execute(get);
            if (httpResponse.getStatusLine().getStatusCode() == HTTP_OK) {
                String response;
                try (BufferedReader buffer = new BufferedReader(new InputStreamReader(httpResponse.getEntity().getContent()))) {
                    response = buffer.lines().collect(Collectors.joining("\n"));
                }
                map = this.gson.fromJson(response, Map.class);
            }
        } finally {
            if (httpResponse != null) {
                httpResponse.close();
            }
        }
        return map;
    }


    @Override
    public void close() throws IOException {
        this.client.close();
    }
}
