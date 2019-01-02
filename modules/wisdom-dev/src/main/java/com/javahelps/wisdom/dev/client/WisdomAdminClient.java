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

import com.google.gson.Gson;
import org.apache.http.NoHttpResponseException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.stream.Collectors;

import static com.javahelps.wisdom.dev.util.Constants.HTTP_OK;

public class WisdomAdminClient extends WisdomHTTPClient {

    private final Gson gson = new Gson();

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

    public Map<String, Comparable> info() throws IOException {

        Map<String, Comparable> map = null;
        HttpGet get = new HttpGet(this.endpoint + "admin/info");
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
}
