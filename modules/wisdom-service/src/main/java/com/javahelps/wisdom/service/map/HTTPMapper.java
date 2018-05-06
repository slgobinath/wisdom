package com.javahelps.wisdom.service.map;

import com.google.gson.Gson;
import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.exception.WisdomAppRuntimeException;
import com.javahelps.wisdom.core.exception.WisdomAppValidationException;
import com.javahelps.wisdom.core.extension.WisdomExtension;
import com.javahelps.wisdom.core.map.Mapper;
import com.javahelps.wisdom.core.util.Commons;
import com.javahelps.wisdom.dev.util.Utility;
import com.javahelps.wisdom.service.exception.WisdomServiceException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static com.javahelps.wisdom.dev.util.Constants.MEDIA_APPLICATION_JSON;

@WisdomExtension("http")
public class HTTPMapper extends Mapper {

    private static final Logger LOGGER = LoggerFactory.getLogger(HTTPMapper.class);

    private final String endpoint;
    private final String select;
    private final String method;
    private CloseableHttpClient client;
    private final Gson gson = new Gson();

    public HTTPMapper(String attrName, Map<String, ?> properties) {
        super(attrName, properties);

        this.endpoint = Commons.getProperty(properties, "endpoint", 0);
        String method = Commons.getProperty(properties, "method", 1);
        this.select = Commons.getProperty(properties, "select", 2);
        if (this.endpoint == null) {
            throw new WisdomAppValidationException("Required property endpoint for HTTP mapper not found");
        }
        if (method == null) {
            throw new WisdomAppValidationException("Required property method for HTTP mapper not found");
        }
        this.method = method.toUpperCase();
        if (!this.method.equals("POST") && !this.method.equals("GET")) {
            throw new WisdomAppValidationException("HTTP mapper property 'method' expects 'POST' or 'GET' but found '%s'", method);
        }
    }

    @Override
    public void start() {
        this.client = HttpClientBuilder.create().build();
    }

    @Override
    public void init(WisdomApp wisdomApp) {

    }

    @Override
    public void stop() {
        LOGGER.info("Closing HTTP mapper");
        try {
            if (this.client != null) {
                this.client.close();
            }
        } catch (IOException e) {
            // Do nothing
        }
    }

    @Override
    public Event map(Event event) {
        try {
            Object val;
            if ("POST" .equals(this.method)) {
                val = this.post(event);
            } else {
                val = this.get(event);
            }
            event.set(this.attrName, val);
        } catch (IOException e) {
            throw new WisdomAppRuntimeException("Error in connecting to " + this.endpoint, e);
        } catch (URISyntaxException e) {
            throw new WisdomAppRuntimeException("Error in constructing URL to " + this.endpoint, e);
        }
        return event;
    }

    private Object post(Event event) throws IOException {

        String json = Utility.toJson(event);
        HttpPost post = new HttpPost(endpoint);
        StringEntity input = new StringEntity(json);
        input.setContentType(MEDIA_APPLICATION_JSON);
        post.setEntity(input);
        CloseableHttpResponse response = client.execute(post);
        try {
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode >= 400) {
                throw new WisdomServiceException(
                        String.format("Error in sending event %s to the endpoint %s. Response: %d, %s",
                                json, this.endpoint, statusCode, response.getStatusLine().getReasonPhrase()));
            } else {
                Map<String, Object> result = this.gson.fromJson(new InputStreamReader(response.getEntity().getContent()), Map.class);
                return result.get(this.select);
            }
        } finally {
            response.close();
        }
    }

    private Object get(Event event) throws IOException, URISyntaxException {

        URIBuilder builder = new URIBuilder(this.endpoint);
        Set<Map.Entry<String, Object>> entrySet = event.getData().entrySet();
        for (Map.Entry<String, Object> entry : entrySet) {
            builder.setParameter(entry.getKey(), Objects.toString(entry.getValue()));
        }

        HttpGet get = new HttpGet(builder.build());

        CloseableHttpResponse response = client.execute(get);
        try {
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode >= 400) {
                throw new WisdomServiceException(
                        String.format("Error in sending event %s to the endpoint %s. Response: %d, %s",
                                event.getData(), this.endpoint, statusCode, response.getStatusLine().getReasonPhrase()));
            } else {
                Map<String, Object> result = this.gson.fromJson(new InputStreamReader(response.getEntity().getContent()), Map.class);
                return result.get(this.select);
            }
        } finally {
            response.close();
        }
    }
}
