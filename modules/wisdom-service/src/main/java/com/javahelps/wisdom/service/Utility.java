package com.javahelps.wisdom.service;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.javahelps.wisdom.core.event.Event;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

public class Utility {

    private static final Gson gson = new Gson();

    private Utility() {
    }

    /**
     * Convert JSON string to java.util.Map.
     *
     * @param jsonString
     * @return
     */
    public static Map<String, Comparable> toMap(String jsonString) {
        System.out.println(jsonString);
        Type type = new TypeToken<Map<String, String>>() {
        }.getType();
        return gson.fromJson(jsonString, type);
    }

    /**
     * Convert given java.util.Map to JSON string.
     *
     * @param data
     * @return
     */
    public static String toJson(Map<String, Comparable> data) {
        return gson.toJson(data);
    }

    /**
     * Create a JSON string containing all attributes of the event.
     *
     * @param event Wisdom event
     * @return JSON string
     */
    public static String toJson(Event event) {
        return gson.toJson(event.getData());
    }

    /**
     * Create a JSON string array containing attributes of all events.
     *
     * @param events a list of Wisdom events
     * @return JSON string
     */
    public static String toJson(List<Event> events) {
        int length = events.size();
        Map<String, Comparable>[] dataArray = new Map[length];
        for (int i = 0; i < length; i++) {
            dataArray[i] = events.get(i).getData();
        }
        return gson.toJson(dataArray);
    }
}
