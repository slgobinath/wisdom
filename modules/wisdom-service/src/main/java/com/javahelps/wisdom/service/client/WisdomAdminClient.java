package com.javahelps.wisdom.service.client;

import org.apache.http.NoHttpResponseException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;

import java.io.IOException;

public class WisdomAdminClient extends WisdomHTTPClient {

    public WisdomAdminClient(String host, int port) {
        super(host, port);
    }

    /**
     * Shutdown Wisdom service.
     * Calling this method does not require explict {@link WisdomHTTPClient#close()} call.
     *
     * @throws IOException
     */
    public void stop() throws IOException {

        try {
            HttpPost post = new HttpPost(this.endpoint + "admin/shutdown");
            CloseableHttpResponse httpResponse = this.client.execute(post);
            httpResponse.close();
        } catch (NoHttpResponseException ex) {
            // Do nothing
        } finally {
            this.close();
        }
    }
}
