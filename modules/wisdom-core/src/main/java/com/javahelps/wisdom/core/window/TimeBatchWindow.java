package com.javahelps.wisdom.core.window;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.exception.WisdomAppValidationException;
import com.javahelps.wisdom.core.extension.WisdomExtension;
import com.javahelps.wisdom.core.processor.Processor;
import com.javahelps.wisdom.core.time.Executor;
import com.javahelps.wisdom.core.time.Scheduler;
import com.javahelps.wisdom.core.time.TimestampGenerator;
import com.javahelps.wisdom.core.variable.Variable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * TimeBatchWindow depending on system timestamp.
 */
@WisdomExtension("timeBatch")
public class TimeBatchWindow extends Window implements Variable.OnUpdateListener<Number>, Executor {

    private long timeToKeep;
    private List<Event> events = new ArrayList<>();
    private long endTime = -1;
    private Variable<Number> timeVariable;
    private Scheduler scheduler;
    private TimestampGenerator timestampGenerator;
    private Processor nextProcessor;

    public TimeBatchWindow(Map<String, ?> properties) {
        super(properties);
        Object durationVal = this.getProperty("duration", 1);

        if (durationVal instanceof Number) {
            this.timeToKeep = ((Number) durationVal).longValue();
        } else if (durationVal instanceof Variable) {
            this.timeVariable = (Variable<Number>) durationVal;
            this.timeToKeep = this.timeVariable.get().longValue();
            this.timeVariable.addOnUpdateListener(this);
        } else {
            throw new WisdomAppValidationException("duration of TimeBatchWindow must be long but found %d",
                    durationVal.getClass().getSimpleName());
        }
    }

    @Override
    public void init(WisdomApp app) {
        this.scheduler = app.getContext().getScheduler();
        this.timestampGenerator = app.getContext().getTimestampGenerator();
    }

    @Override
    public void process(Event event, Processor nextProcessor) {

        long currentTimestamp = this.timestampGenerator.currentTimestamp();

        try {
            this.lock.lock();
            if (this.endTime == -1) {
                this.endTime = currentTimestamp + this.timeToKeep;
                this.scheduler.schedule(Duration.ofMillis(this.timeToKeep), this);
            } else if (currentTimestamp >= this.endTime) {
                this.endTime += this.timeToKeep;
                this.scheduler.schedule(Duration.ofMillis(this.endTime - currentTimestamp), this);
            }
            this.events.add(event);
            this.nextProcessor = nextProcessor;
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public Window copy() {
        return new TimeBatchWindow(this.properties);
    }

    @Override
    public void clear() {
        try {
            this.lock.lock();
            this.events.clear();
            this.endTime = -1;
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public void destroy() {
        if (this.timeVariable != null) {
            this.timeVariable.removeOnUpdateListener(this);
        }
        this.events = null;
    }

    @Override
    public void update(Number value) {
        try {
            this.lock.lock();
            long val = value.longValue();
            this.endTime = this.endTime - this.timeToKeep + val;
            this.timeToKeep = val;
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public void execute(long timestamp) {
        List<Event> eventsToSend = null;
        Processor nextProcessor;
        try {
            this.lock.lock();
            nextProcessor = this.nextProcessor;
            if (timestamp >= this.endTime && !this.events.isEmpty()) {
                // Timeout happened
                eventsToSend = new ArrayList<>(this.events);
                this.events.clear();
            }
        } finally {
            this.lock.unlock();
        }
        if (eventsToSend != null && nextProcessor != null) {
            nextProcessor.process(eventsToSend);
        }
    }
}
