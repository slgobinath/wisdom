package com.javahelps.wisdom.dev.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.javahelps.wisdom.dev.json.AnnotatedJsonDeserializer;

import java.lang.reflect.Type;

public class MappingUtil {

    private MappingUtil() {

    }

    public static Gson annotatedGson(Type... types) {
        GsonBuilder builder = new GsonBuilder();
        AnnotatedJsonDeserializer deserializer = new AnnotatedJsonDeserializer<>();
        for (Type type : types) {
            builder.registerTypeAdapter(type, deserializer);
        }
        return builder.create();
    }
}
