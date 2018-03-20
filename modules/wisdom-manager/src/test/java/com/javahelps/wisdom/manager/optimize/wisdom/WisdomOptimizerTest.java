package com.javahelps.wisdom.manager.optimize.wisdom;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.extension.ImportsManager;
import com.javahelps.wisdom.core.stream.InputHandler;
import com.javahelps.wisdom.core.stream.StreamCallback;
import com.javahelps.wisdom.core.util.EventGenerator;
import com.javahelps.wisdom.core.util.EventPrinter;
import com.javahelps.wisdom.extensions.unique.window.UniqueExternalTimeBatchWindow;
import com.javahelps.wisdom.manager.optimize.multivariate.Constraint;
import com.javahelps.wisdom.manager.optimize.multivariate.Point;
import com.javahelps.wisdom.query.WisdomCompiler;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class WisdomOptimizerTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(WisdomOptimizerTest.class);

    static {
        ImportsManager.INSTANCE.use("unique:externalTimeBatch", UniqueExternalTimeBatchWindow.class);
    }

    @Test
    public void testOptimizer1() throws IOException {

        LOGGER.info("Test WisdomOptimizer for IPSweep");
        Path path = Paths.get(WisdomOptimizer.class.getClassLoader().getResource("ip_sweep.wisdomql").getPath());
        WisdomApp app = WisdomCompiler.parse(path);
        WisdomOptimizer manager = new WisdomOptimizer(app);
        manager.addTrainable("time_threshold", new Constraint(100L, 5000L), -1);
        manager.addTrainable("count_threshold", new Constraint(5L, 1000L), 1);

        IPSweepTrainer trainer = new IPSweepTrainer();
        manager.addQueryTrainer(trainer);

        app.start();
        Point point = manager.optimize();

        long timestamp = (long) point.getCoordinates()[0];
        long count = (long) point.getCoordinates()[1];

        LOGGER.info("Optimized timestamp: {} and count: {}", timestamp, count);


        Assert.assertEquals("Query is not fully optimized", 0.0, trainer.loss(), 0.0);
        app.shutdown();
    }

    static class IPSweepTrainer implements QueryTrainer, StreamCallback {

        private InputHandler inputHandler;
        private List<Event> receivedEvents = new ArrayList<>();

        @Override
        public void init(WisdomApp app) {
            app.addCallback("IPSweepStream", this);
            this.inputHandler = app.getInputHandler("PacketStream");
        }

        @Override
        public void train() {
            long timestamp = 0;
            synchronized (this) {
                this.receivedEvents.clear();
            }
            for (int i = 0; i < 1000; i++) {
                timestamp += 2;
                this.inputHandler.send(EventGenerator.generate("protocol", "icmp", "srcIp", "127.0.0.1", "destIp", "127.0.0." + i, "timestamp", timestamp));
            }
        }

        @Override
        public double loss() {
            double loss = 1000.0;
            int noOfEvents = this.receivedEvents.size();
            if (noOfEvents == 1) {
                loss -= 100;
                Event event = this.receivedEvents.get(0);
                if ("127.0.0.1".equals(event.get("srcIp"))) {
                    loss = 0.0;
                }
            }
            return loss;
        }

        @Override
        public void receive(Event... events) {
            EventPrinter.print(events);
            synchronized (this) {
                receivedEvents.addAll(Arrays.asList(events));
            }
        }
    }
}
