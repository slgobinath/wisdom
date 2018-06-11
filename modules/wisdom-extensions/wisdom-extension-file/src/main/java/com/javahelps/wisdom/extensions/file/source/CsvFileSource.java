package com.javahelps.wisdom.extensions.file.source;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.exception.WisdomAppRuntimeException;
import com.javahelps.wisdom.core.exception.WisdomAppValidationException;
import com.javahelps.wisdom.core.extension.WisdomExtension;
import com.javahelps.wisdom.core.stream.InputHandler;
import com.javahelps.wisdom.core.stream.input.Source;
import com.javahelps.wisdom.core.util.Commons;
import com.javahelps.wisdom.core.util.EventGenerator;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import static com.javahelps.wisdom.extensions.file.util.Constants.PATH;

@WisdomExtension("file.csv")
public class CsvFileSource extends Source {

    private final String path;
    private boolean running;
    private InputHandler inputHandler;
    private final List<BiConsumer<CSVRecord, Map<String, Object>>> consumers = new ArrayList<>();
    private final int noOfAttributes;

    public CsvFileSource(Map<String, ?> properties) {
        super(properties);
        this.path = Commons.getProperty(properties, PATH, 0);
        if (this.path == null) {
            throw new WisdomAppValidationException("Required property %s for CsvFile sink not found", PATH);
        }
        for (Map.Entry<String, ?> entry : properties.entrySet()) {
            final String attribute = entry.getKey();
            if (!PATH.equals(entry.getKey()) && !"type".equals(entry.getKey())) {
                String dataType = (String) entry.getValue();
                if ("int".equals(dataType)) {
                    consumers.add((record, map) -> map.put(attribute, Integer.valueOf(record.get(attribute))));
                } else if ("long".equals(dataType)) {
                    consumers.add((record, map) -> map.put(attribute, Long.valueOf(record.get(attribute))));
                } else if ("float".equals(dataType)) {
                    consumers.add((record, map) -> map.put(attribute, Float.valueOf(record.get(attribute))));
                } else if ("double".equals(dataType)) {
                    consumers.add((record, map) -> map.put(attribute, Double.valueOf(record.get(attribute))));
                } else if ("bool".equals(dataType)) {
                    consumers.add((record, map) -> map.put(attribute, Boolean.valueOf(record.get(attribute))));
                } else {
                    consumers.add((record, map) -> {
                        map.put(attribute, record.get(attribute));
                    });
                }
            }
        }
        this.noOfAttributes = this.consumers.size();
    }


    @Override
    public void init(WisdomApp wisdomApp, String streamId) {
        this.inputHandler = wisdomApp.getInputHandler(streamId);
    }

    @Override
    public void start() {
        synchronized (this) {
            if (this.running) {
                return;
            }
            this.running = true;
        }

        CSVParser parser;
        try {
            parser = CSVParser.parse(Paths.get(this.path), Charset.defaultCharset(), CSVFormat.DEFAULT.withFirstRecordAsHeader());
        } catch (IOException e) {
            throw new WisdomAppRuntimeException("Could not read csv file: %s", path);
        }

        for (CSVRecord record : parser) {
            synchronized (this) {
                if (!this.running) {
                    break;
                }
            }
            Map<String, Object> map = new HashMap<>(this.noOfAttributes);
            for (BiConsumer<CSVRecord, Map<String, Object>> consumer : this.consumers) {
                consumer.accept(record, map);
            }
            this.inputHandler.send(EventGenerator.generate(map));
        }

        try {
            parser.close();
        } catch (IOException e) {
            throw new WisdomAppRuntimeException("Error in closing csv file: %s", path);
        }

        synchronized (this) {
            this.running = false;
        }
    }

    @Override
    public void stop() {
        synchronized (this) {
            this.running = false;
        }
    }
}
