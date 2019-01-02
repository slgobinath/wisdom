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

package com.javahelps.wisdom.core.extension;

import com.javahelps.wisdom.core.exception.WisdomAppValidationException;
import com.javahelps.wisdom.core.function.event.EventFunction;
import com.javahelps.wisdom.core.map.Mapper;
import com.javahelps.wisdom.core.operator.AggregateOperator;
import com.javahelps.wisdom.core.operator.logical.LogicalOperator;
import com.javahelps.wisdom.core.stream.input.Source;
import com.javahelps.wisdom.core.stream.output.Sink;
import com.javahelps.wisdom.core.window.Window;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.scanners.TypeAnnotationsScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public enum ImportsManager {

    INSTANCE;

    private final Map<String, Constructor> windows = new HashMap<>();
    private final Map<String, Constructor> sinks = new HashMap<>();
    private final Map<String, Constructor> sources = new HashMap<>();
    private final Map<String, Constructor> mappers = new HashMap<>();
    private final Map<String, Constructor> logicalOperators = new HashMap<>();
    private final Map<String, Constructor> aggregateOperators = new HashMap<>();
    private final Map<String, Constructor> eventFunctions = new HashMap<>();

    public void use(Class<?> clazz) {

        WisdomExtension annotation = clazz.getAnnotation(WisdomExtension.class);
        if (annotation == null) {
            throw new WisdomAppValidationException("Class %s is not annotated by @WisdomExtension", clazz.getCanonicalName());
        }
        String namespace = annotation.value();

        if (Window.class.isAssignableFrom(clazz)) {
            try {
                this.windows.put(namespace, clazz.getConstructor(Map.class));
            } catch (NoSuchMethodException e) {
                throw new WisdomAppValidationException("<init>(java.util.Map<String, ?>) not found in %s", clazz.getCanonicalName());
            }
        } else if (Sink.class.isAssignableFrom(clazz)) {
            try {
                this.sinks.put(namespace, clazz.getConstructor(Map.class));
            } catch (NoSuchMethodException e) {
                throw new WisdomAppValidationException("<init>(java.util.Map<String, ?>) not found in %s", clazz.getCanonicalName());
            }
        } else if (Source.class.isAssignableFrom(clazz)) {
            try {
                this.sources.put(namespace, clazz.getConstructor(Map.class));
            } catch (NoSuchMethodException e) {
                throw new WisdomAppValidationException("<init>(java.util.Map<String, ?>) not found in %s", clazz.getCanonicalName());
            }
        } else if (Mapper.class.isAssignableFrom(clazz)) {
            try {
                this.mappers.put(namespace, clazz.getConstructor(String.class, Map.class));
            } catch (NoSuchMethodException e) {
                throw new WisdomAppValidationException("<init>(java.lang.String, java.util.Map<String, ?>) not found in %s", clazz.getCanonicalName());
            }
        } else if (LogicalOperator.class.isAssignableFrom(clazz)) {
            try {
                this.logicalOperators.put(namespace, clazz.getConstructor(Map.class));
            } catch (NoSuchMethodException e) {
                throw new WisdomAppValidationException("<init>(java.util.Map<String, ?>) not found in %s", clazz.getCanonicalName());
            }
        } else if (AggregateOperator.class.isAssignableFrom(clazz)) {
            try {
                this.aggregateOperators.put(namespace, clazz.getConstructor(String.class, Map.class));
            } catch (NoSuchMethodException e) {
                throw new WisdomAppValidationException("<init>(java.lang.String, java.util.Map<String, ?>) not found in %s", clazz.getCanonicalName());
            }
        } else if (EventFunction.class.isAssignableFrom(clazz)) {
            try {
                this.eventFunctions.put(namespace, clazz.getConstructor(Map.class));
            } catch (NoSuchMethodException e) {
                throw new WisdomAppValidationException("<init>(java.util.Map<String, ?>) not found in %s", clazz.getCanonicalName());
            }
        }
    }

    public void use(String packageName) {

        Reflections reflections = new Reflections(packageName);
        Set<Class<? extends Object>> extensionClasses = reflections.getTypesAnnotatedWith(WisdomExtension.class);
        for (Class<?> clazz : extensionClasses) {
            this.use(clazz);
        }
    }

    public Window createWindow(String namespace, Map<String, ?> properties) {
        Constructor constructor = this.windows.get(namespace);
        if (constructor == null) {
            throw new WisdomAppValidationException("Class to create %s window was not imported", namespace);
        }
        try {
            return (Window) constructor.newInstance(properties);
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new WisdomAppValidationException(e.getCause(), "Failed to create %s window instance", namespace);
        }
    }

    public Sink createSink(String namespace, Map<String, ?> properties) {
        Constructor constructor = this.sinks.get(namespace);
        if (constructor == null) {
            throw new WisdomAppValidationException("Class to create %s sink was not imported", namespace);
        }
        try {
            return (Sink) constructor.newInstance(properties);
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new WisdomAppValidationException(e.getCause(), "Failed to create %s sink instance", namespace);
        }
    }

    public Source createSource(String namespace, Map<String, ?> properties) {
        Constructor constructor = this.sources.get(namespace);
        if (constructor == null) {
            throw new WisdomAppValidationException("Class to create %s source was not imported", namespace);
        }
        try {
            return (Source) constructor.newInstance(properties);
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new WisdomAppValidationException(e.getCause(), "Failed to create %s source instance", namespace);
        }
    }

    public Mapper createMapper(String namespace, String newName, Map<String, ?> properties) {
        Constructor constructor = this.mappers.get(namespace);
        if (constructor == null) {
            throw new WisdomAppValidationException("Class to create %s mapper was not imported", namespace);
        }
        try {
            return (Mapper) constructor.newInstance(newName, properties);
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new WisdomAppValidationException(e.getCause(), "Failed to create %s mapper instance", namespace);
        }
    }

    public LogicalOperator createLogicalOperator(String namespace, Map<String, ?> properties) {
        Constructor constructor = this.logicalOperators.get(namespace);
        if (constructor == null) {
            throw new WisdomAppValidationException("Class to create %s mapper was not imported", namespace);
        }
        try {
            return (LogicalOperator) constructor.newInstance(properties);
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new WisdomAppValidationException(e.getCause(), "Failed to create %s logical operator instance", namespace);
        }
    }

    public AggregateOperator createAggregateOperator(String namespace, String newName, Map<String, ?> properties) {
        Constructor constructor = this.aggregateOperators.get(namespace);
        if (constructor == null) {
            throw new WisdomAppValidationException("Class to create %s mapper was not imported", namespace);
        }
        try {
            return (AggregateOperator) constructor.newInstance(newName, properties);
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new WisdomAppValidationException(e.getCause(), "Failed to create %s aggregator operator instance", namespace);
        }
    }

    public EventFunction createEventFunction(String namespace, Map<String, ?> properties) {
        Constructor constructor = this.eventFunctions.get(namespace);
        if (constructor == null) {
            throw new WisdomAppValidationException("Class to create %s event function was not imported", namespace);
        }
        try {
            return (EventFunction) constructor.newInstance(properties);
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new WisdomAppValidationException(e.getCause(), "Failed to create %s event function instance", namespace);
        }
    }

    public void scanClassPath() {
        ConfigurationBuilder builder = new ConfigurationBuilder()
                .addUrls(ClasspathHelper.forJavaClassPath())
                .setScanners(new TypeAnnotationsScanner(), new SubTypesScanner());
        Reflections reflections = new Reflections(builder);

        Set<Class<?>> extensions = reflections.getTypesAnnotatedWith(WisdomExtension.class);
        for (Class<?> clazz : extensions) {
            this.use(clazz);
        }
    }
}
