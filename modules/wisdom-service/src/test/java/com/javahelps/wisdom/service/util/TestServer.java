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

package com.javahelps.wisdom.service.util;

import spark.Spark;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

import static com.javahelps.wisdom.dev.util.Constants.MEDIA_APPLICATION_JSON;

public class TestServer {

    private static final String OUTPUT_FILE = "test_server_output.log";
    private static final int PORT = 9999;
    private static long waitingTime = 10_000L;

    private static void appendToFile(String data) {

        try {
            Files.write(Paths.get(OUTPUT_FILE), Arrays.asList(data), StandardOpenOption.APPEND);
        } catch (IOException e) {
        }
    }

    public static void main(String[] args) {
        if (args.length > 0) {
            try {
                long value = Long.parseLong(args[0]);
                waitingTime = value;
            } catch (NumberFormatException e) {
            }
        }
        new TestServer().start();
    }

    private synchronized void start() {

        appendToFile("INFO: Starting test server");
        Spark.port(PORT);
        Spark.post("/streamReceiver", MEDIA_APPLICATION_JSON, (request, response) -> {
            try {
                appendToFile(request.body());
                response.status(200);
            } catch (Exception ex) {
                appendToFile(ex.getMessage());
            }
            return response;
        });

        new Thread(() -> {
            sleep(waitingTime);
            this.stop();
        }).start();
    }

    private synchronized void stop() {

        appendToFile("INFO: Shutting down test server");
        try {
            Spark.stop();
        } catch (IllegalStateException ex) {
        }
        sleep(100);
        System.exit(0);
    }

    private void sleep(long timestamp) {
        try {
            Thread.sleep(timestamp);
        } catch (InterruptedException e) {
        }
    }
}
