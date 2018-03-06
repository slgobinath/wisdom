package com.javahelps.wisdom.core.extension;

import com.javahelps.wisdom.core.exception.WisdomAppValidationException;
import com.javahelps.wisdom.core.window.Window;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

public enum ImportsManager {

    INSTANCE;

    private final Map<String, Constructor> windows = new HashMap();

    public void use(String namespace, Class<?> clazz) {

        if (Window.class.isAssignableFrom(clazz)) {
            try {
                this.windows.put(namespace, clazz.getConstructor(Map.class));
            } catch (NoSuchMethodException e) {
                e.printStackTrace();
                throw new WisdomAppValidationException("<init>(java.util.Map<String, ?>) not found in %s", clazz.getCanonicalName());
            }
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
            e.printStackTrace();
            throw new WisdomAppValidationException("Failed to create %s window instance", namespace);
        }
    }
}
