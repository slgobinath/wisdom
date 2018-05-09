package com.javahelps.wisdom.core.stream;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.exception.WisdomAppRuntimeException;
import com.javahelps.wisdom.core.processor.Processor;
import com.javahelps.wisdom.core.time.EventBasedTimestampGenerator;

/**
 * InputHandler to send events into a stream.
 */
public class InputHandler {

    private final Processor processor;
    private final WisdomApp wisdomApp;
    private EventBasedTimestampGenerator timestampGenerator;
    private String playbackTimestamp;

    public InputHandler(Processor processor, WisdomApp wisdomApp) {
        this.processor = processor;
        this.wisdomApp = wisdomApp;
        if (wisdomApp.getContext().isPlaybackEnabled()) {
            this.timestampGenerator = (EventBasedTimestampGenerator) wisdomApp.getContext().getTimestampGenerator();
            this.playbackTimestamp = wisdomApp.getContext().getPlaybackAttribute();
        }
    }

    public void send(Event event) {
        if (this.timestampGenerator != null) {
            Object timestamp = event.get(this.playbackTimestamp);
            if (timestamp != null && timestamp instanceof Number) {
                this.timestampGenerator.setCurrentTimestamp(((Number) timestamp).longValue());
            }
        }
        try {
            this.processor.process(event);
        } catch (WisdomAppRuntimeException ex) {
            this.wisdomApp.handleException(ex);
        }
    }
}
