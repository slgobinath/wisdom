package com.javahelps.wisdom.extensions.file.sink;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.exception.WisdomAppValidationException;
import com.javahelps.wisdom.core.extension.WisdomExtension;
import com.javahelps.wisdom.core.stream.output.Sink;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;

import static com.javahelps.wisdom.extensions.file.sink.com.javahelps.wisdom.extensions.file.util.Constants.PATH;

@WisdomExtension("file.text")
public class TextFileSink extends Sink {

    private final String path;

    public TextFileSink(Map<String, ?> properties) {
        super(properties);
        this.path = (String) properties.get(PATH);
        if (this.path == null) {
            throw new WisdomAppValidationException("Required property %s for TextFile sink not found", PATH);
        }
    }

    @Override
    public void start() {

    }

    @Override
    public void init(WisdomApp wisdomApp, String streamId) {

    }

    @Override
    public void publish(List<Event> events) throws IOException {
        try (PrintWriter writer = new PrintWriter(new FileWriter(this.path, true))) {
            for (Event event : events) {
                writer.println(event);
            }
        }
    }

    @Override
    public void stop() {

    }
}
