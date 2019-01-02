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

package com.javahelps.wisdom.extensions.unique.window;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.extension.ImportsManager;
import com.javahelps.wisdom.core.stream.InputHandler;
import com.javahelps.wisdom.core.util.EventGenerator;
import com.javahelps.wisdom.core.window.Window;
import com.javahelps.wisdom.dev.test.TestCallback;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.javahelps.wisdom.core.util.Commons.map;

public class UniqueLengthBatchWindowTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(UniqueLengthBatchWindowTest.class);

    static {
        ImportsManager.INSTANCE.use(UniqueLengthBatchWindow.class);
    }

    private TestCallback callbackUtil = new TestCallback(LOGGER);

    @Test
    public void testWindow1() {
        LOGGER.info("Test window 1 - OUT 3");

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("LoginEventStream");
        wisdomApp.defineStream("OutputStream");

        wisdomApp.defineQuery("query1")
                .from("LoginEventStream")
                .window(Window.create("unique:lengthBatch", map("uniqueKey", "ip", "length", 3)))
                .select("ip", "timestamp")
                .insertInto("OutputStream");

        TestCallback.TestResult testResult = callbackUtil.addCallback(wisdomApp, "OutputStream",
                map("ip", "192.10.1.3", "timestamp", 1366335804341L),
                map("ip", "192.10.1.4", "timestamp", 1366335805341L),
                map("ip", "192.10.1.5", "timestamp", 1366335814341L));

        wisdomApp.start();

        InputHandler loginEventStream = wisdomApp.getInputHandler("LoginEventStream");
        loginEventStream.send(EventGenerator.generate("ip", "192.10.1.3", "timestamp", 1366335804341L));
        loginEventStream.send(EventGenerator.generate("ip", "192.10.1.4", "timestamp", 1366335804342L));
        loginEventStream.send(EventGenerator.generate("ip", "192.10.1.4", "timestamp", 1366335805341L));
        loginEventStream.send(EventGenerator.generate("ip", "192.10.1.5", "timestamp", 1366335814341L));
        loginEventStream.send(EventGenerator.generate("ip", "192.10.1.6", "timestamp", 1366335814345L));
        loginEventStream.send(EventGenerator.generate("ip", "192.10.1.7", "timestamp", 1366335824341L));

        wisdomApp.shutdown();

        testResult.assertTestResult(3);
    }

    @Test
    public void testWindow2() {
        LOGGER.info("Test window 2 - OUT 2");

        WisdomApp wisdomApp = new WisdomApp();
        wisdomApp.defineStream("LoginEventStream");
        wisdomApp.defineStream("OutputStream");

        wisdomApp.defineQuery("query1")
                .from("LoginEventStream")
                .window(Window.create("unique:lengthBatch", map("uniqueKey", "ip", "length", 2)))
                .select("ip", "timestamp")
                .insertInto("OutputStream");

        TestCallback.TestResult testResult = callbackUtil.addCallback(wisdomApp, "OutputStream",
                map("ip", "192.10.1.3", "timestamp", 1366335804341L),
                map("ip", "192.10.1.4", "timestamp", 1366335804342L),
                map("ip", "192.10.1.5", "timestamp", 1366335814641L),
                map("ip", "192.10.1.6", "timestamp", 1366335814545L));

        wisdomApp.start();

        InputHandler loginEventStream = wisdomApp.getInputHandler("LoginEventStream");
        loginEventStream.send(EventGenerator.generate("ip", "192.10.1.3", "timestamp", 1366335804341L));
        loginEventStream.send(EventGenerator.generate("ip", "192.10.1.4", "timestamp", 1366335804342L));
        loginEventStream.send(EventGenerator.generate("ip", "192.10.1.5", "timestamp", 1366335814341L));
        loginEventStream.send(EventGenerator.generate("ip", "192.10.1.5", "timestamp", 1366335814741L));
        loginEventStream.send(EventGenerator.generate("ip", "192.10.1.5", "timestamp", 1366335814641L));
        loginEventStream.send(EventGenerator.generate("ip", "192.10.1.6", "timestamp", 1366335814545L));
        loginEventStream.send(EventGenerator.generate("ip", "192.10.1.7", "timestamp", 1366335824341L));

        wisdomApp.shutdown();

        testResult.assertTestResult(4);
    }
}
