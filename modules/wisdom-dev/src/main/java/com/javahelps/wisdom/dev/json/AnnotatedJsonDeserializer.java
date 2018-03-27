package com.javahelps.wisdom.dev.json;

import com.google.gson.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Type;

public class AnnotatedJsonDeserializer<T> implements JsonDeserializer<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AnnotatedJsonDeserializer.class);

    private final Gson gson = new Gson();

    @Override
    public T deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {
        T pojo = this.gson.fromJson(jsonElement, type);
        Field[] fields = pojo.getClass().getDeclaredFields();
        for (Field f : fields) {
            if (f.getAnnotation(JsonRequired.class) != null) {
                try {
                    f.setAccessible(true);
                    if (f.get(pojo) == null) {
                        throw new JsonParseException("Missing field in JSON: " + f.getName());
                    }
                } catch (IllegalArgumentException | IllegalAccessException e) {
                    LOGGER.error("Error in parsing JSON string", e);
                }
            }
        }
        return pojo;
    }
}
