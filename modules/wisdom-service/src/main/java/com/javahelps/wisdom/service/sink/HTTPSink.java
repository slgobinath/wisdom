package com.javahelps.wisdom.service.sink;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.exception.WisdomAppValidationException;
import com.javahelps.wisdom.core.extension.WisdomExtension;
import com.javahelps.wisdom.core.stream.output.Sink;
import com.javahelps.wisdom.service.Utility;
import com.javahelps.wisdom.service.exception.WisdomServiceException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.javahelps.wisdom.service.Constant.*;
import static java.util.Map.entry;

@WisdomExtension("http")
public class HTTPSink extends Sink {

    private static final Logger LOGGER = LoggerFactory.getLogger(HTTPSink.class);

    private final String endpoint;
    private final boolean batch;
    private final CloseableHttpClient client = HttpClientBuilder.create().build();

    public HTTPSink(String endpoint) {
        this(endpoint, false);
    }

    public HTTPSink(String endpoint, boolean batch) {
        this(Map.ofEntries(entry(ENDPOINT, endpoint), entry(BATCH, batch)));
    }

    public HTTPSink(Map<String, Comparable> properties) {
        super(properties);
        this.endpoint = (String) properties.get(ENDPOINT);
        if (this.endpoint == null) {
            throw new WisdomAppValidationException("Required property %s for HTTP sink not found", ENDPOINT);
        }
        this.batch = (boolean) properties.getOrDefault(BATCH, false);
    }

    @Override
    public void start() {

    }

    @Override
    public void init(WisdomApp wisdomApp, String streamId) {

    }

    @Override
    public void publish(List<Event> events) throws IOException {

        try {
            if (this.batch) {
                this.publish(Utility.toJson(events));
            } else {
                for (Event event : events) {
                    this.publish(Utility.toJson(event));
                }
            }
        } catch (WisdomServiceException ex) {
            LOGGER.error("Failed to send HTTP event", ex);
        }
    }

    @Override
    public void stop() {
        LOGGER.info("Closing HTTP sink");
        try {
            this.client.close();
        } catch (IOException e) {
            // Do nothing
        }
    }

    private void publish(String json) throws IOException {
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
            }
        } finally {
            response.close();
        }
    }
}
