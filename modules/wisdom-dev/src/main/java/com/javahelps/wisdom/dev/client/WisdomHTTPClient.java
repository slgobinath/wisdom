/*
 * Copyright (c) 2018, Gobinath Loganathan (http://github.com/slgobinath) All Rights Reserved.
 *
 * Gobinath licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. In addition, if you are using
 * this file in your research work, you are required to cite
 * WISDOM as mentioned at https://github.com/slgobinath/wisdom.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.javahelps.wisdom.dev.client;

import com.javahelps.wisdom.dev.util.Utility;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.javahelps.wisdom.dev.util.Constants.MEDIA_APPLICATION_JSON;

public class WisdomHTTPClient extends WisdomClient {

    protected final String endpoint;
    protected final CloseableHttpClient client;

    public WisdomHTTPClient(String host, int port) {
        this.endpoint = String.format("http://%s:%d/WisdomApp/", host, port);
        this.client = HttpClientBuilder.create().setConnectionTimeToLive(0, TimeUnit.MILLISECONDS).build();
    }

    @Override
    public Response send(String streamId, Map<String, Object> data) throws IOException {

        HttpPost post = new HttpPost(this.endpoint + streamId);
        StringEntity input = new StringEntity(Utility.toJson(data));
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

    @Override
    public void close() throws IOException {
        this.client.close();
    }
}
