package com.javahelps.wisdom.extensions.ml.tf;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.exception.WisdomAppRuntimeException;
import com.javahelps.wisdom.core.exception.WisdomAppValidationException;
import com.javahelps.wisdom.core.extension.WisdomExtension;
import com.javahelps.wisdom.core.map.Mapper;
import com.javahelps.wisdom.core.util.Commons;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tensorflow.*;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.javahelps.wisdom.extensions.ml.util.Constants.*;

@WisdomExtension("tensorFlow")
public class TensorFlowMapper extends Mapper {

    private final String path;
    private final String operation;
    private final String type;
    private final Function<Tensor, Comparable> mapper;
    private SavedModelBundle savedModelBundle;
    private Session session;

    private static final Logger LOGGER = LoggerFactory.getLogger(TensorFlowMapper.class);

    public TensorFlowMapper(String attrName, Map<String, ?> properties) {
        super(attrName, properties);
        this.path = Commons.getProperty(properties, PATH, 0);
        this.operation = Commons.getProperty(properties, OPERATION, 1);
        this.type = Commons.getProperty(properties, TYPE, 2);
        if (this.path == null) {
            throw new WisdomAppValidationException("Required property %s for TensorFlow mapper not found", PATH);
        }
        if (this.operation == null) {
            throw new WisdomAppValidationException("Required property %s for TensorFlow mapper not found", OPERATION);
        }
        if (this.type == null) {
            throw new WisdomAppValidationException("Required property %s for TensorFlow mapper not found", TYPE);
        } else {
            if ("int".equalsIgnoreCase(type)) {
                this.mapper = TensorFlowMapper::toInt;
            } else if ("long".equalsIgnoreCase(type)) {
                this.mapper = TensorFlowMapper::toLong;
            } else if ("float".equalsIgnoreCase(type)) {
                this.mapper = TensorFlowMapper::toFloat;
            } else if ("double".equalsIgnoreCase(type)) {
                this.mapper = TensorFlowMapper::toDouble;
            } else if ("bool".equalsIgnoreCase(type)) {
                this.mapper = TensorFlowMapper::toBool;
            } else {
                throw new WisdomAppValidationException("TensorFlow mapper property %s must be 'int', 'long', 'float', 'double' or 'bool' but found ", TYPE, this.type);
            }
        }
    }

    @Override
    public void start() {
        LOGGER.debug("Loading TensorFlow model from {}", this.path);
        try {
            this.savedModelBundle = SavedModelBundle.load(this.path, "serve");
            this.session = this.savedModelBundle.session();
        } catch (TensorFlowException ex) {
            throw new WisdomAppRuntimeException("Failed to load TensorFlow model from " + this.path, ex);
        }
    }

    @Override
    public void init(WisdomApp wisdomApp) {
        // Do nothing
    }

    @Override
    public void stop() {
        if (this.savedModelBundle != null) {
            this.session.close();
            this.savedModelBundle.close();
            this.session = null;
            this.savedModelBundle = null;
        }
    }

    public static void main(String[] args) {
        LOGGER.info("Loading TensorFlow version {}", TensorFlow.version());
        try (SavedModelBundle savedModelBundle = SavedModelBundle.load("/home/gobinath/Workspace/tf_serve/models/hello_world/1", "serve")) {
            System.out.println("Loaded");
            Session session = savedModelBundle.session();
            System.out.println(session.runner().feed("x", Tensor.create(10)).feed("y", Tensor.create(20)).fetch("ans").run().get(0));
        }
    }

    @Override
    public Event map(Event event) {
        Session.Runner runner = this.session.runner();
        for (Map.Entry<String, Comparable> attr : event.getData().entrySet()) {
            runner = runner.feed(attr.getKey(), Tensor.create(attr.getValue()));
        }
        List<Tensor<?>> tensors = runner.fetch(this.operation).run();
        event.set(this.attrName, this.mapper.apply(tensors.get(0)));
        return event;
    }

    private static Comparable toInt(Tensor tensor) {
        return (long) tensor.intValue();
    }

    private static Comparable toFloat(Tensor tensor) {
        return (double) tensor.floatValue();
    }

    private static Comparable toLong(Tensor tensor) {
        return tensor.longValue();
    }

    private static Comparable toDouble(Tensor tensor) {
        return tensor.doubleValue();
    }

    private static Comparable toBool(Tensor tensor) {
        return tensor.booleanValue();
    }
}
