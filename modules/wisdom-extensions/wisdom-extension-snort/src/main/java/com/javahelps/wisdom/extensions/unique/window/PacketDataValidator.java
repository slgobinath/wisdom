package com.javahelps.wisdom.extensions.unique.window;

import com.javahelps.wisdom.core.event.Event;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PacketDataValidator implements Predicate<Event> {

    private final String attribute;
    private Operation lastOperation;
    private String reqexQuery = "";
    private Pattern pattern;
    private int group;
    private Map<Integer, Integer> within = new HashMap<>();
    private Map<Integer, Integer> depth = new HashMap<>();
    private boolean hasWithin;
    private boolean hasDepth;
    private PacketDataValidator(String attribute) {
        this.attribute = attribute;
    }

    public static PacketDataValidator construct(String attribute, Path path) {

        final PacketDataValidator[] holder = new PacketDataValidator[1];
        try {
            Files.lines(path)
                    .map(String::trim)
                    .filter(line -> !line.isEmpty())
                    .filter(line -> !line.startsWith("#"))
                    .forEach(line -> construct(attribute, holder, line));
        } catch (IOException e) {
            e.printStackTrace();
        }
        holder[0].start();
        return holder[0];
    }

    private static void construct(String attribute, PacketDataValidator[] holder, String rule) {

        PacketDataValidator validator = new PacketDataValidator(attribute);
        String[] components = rule.split(";");
        for (String component : components) {
            if (component.startsWith("|")) {
                validator.pattern(component.replaceAll("\\|", ""));
            } else if (component.startsWith("depth")) {
                int depth = extract("depth", component);
                validator.depth(depth);
            } else if (component.startsWith("distance")) {
                int depth = extract("distance", component);
                validator.depth(depth);
            } else if (component.startsWith("within")) {
                int depth = extract("within", component);
                validator.depth(depth);
            }
        }
        if (holder[0] == null) {
            holder[0] = validator;
        } else {
            holder[0] = holder[0].or(validator);
        }
    }

    private static int extract(String type, String component) {
        component = component
                .toLowerCase()
                .replaceAll(type, "")
                .replaceAll(":", "")
                .trim();
        return Integer.parseInt(component);
    }

    public PacketDataValidator pattern(String pattern) {
        this.reqexQuery += ".*?(" + pattern + ")";
        this.lastOperation = Operation.PATTERN;
        this.group++;
        return this;
    }

    public PacketDataValidator distance(int distance) {
        if (lastOperation != Operation.PATTERN) {
            throw new RuntimeException("Distance must follow a pattern");
        }
        this.reqexQuery += String.format(".{%d}.*", distance);
        this.lastOperation = Operation.DISTANCE;
        return this;
    }

    public PacketDataValidator within(int within) {
        if (lastOperation != Operation.PATTERN) {
            throw new RuntimeException("Within must follow a pattern");
        }
        this.within.put(this.group, within);
        this.lastOperation = Operation.WITHIN;
        return this;
    }

    public PacketDataValidator depth(int depth) {
        if (lastOperation != Operation.PATTERN) {
            throw new RuntimeException("Depth must follow a pattern");
        }
        this.depth.put(this.group, depth);
        this.lastOperation = Operation.DEPTH;
        return this;
    }

    public void start() {
        if (lastOperation == Operation.DISTANCE) {
            throw new RuntimeException("Distance cannot be the last entity in a pattern");
        }
        this.pattern = Pattern.compile(this.reqexQuery);
        this.hasWithin = !this.within.isEmpty();
        this.hasDepth = !this.depth.isEmpty();
    }

    public int complexity() {
        return this.reqexQuery.length();
    }

    @Override
    public boolean test(Event event) {
        Object data = event.get(this.attribute);
        if (data == null) {
            return false;
        }
        Matcher matcher = this.pattern.matcher(data.toString());
        while (matcher.find()) {
            if (this.group == matcher.groupCount()) {
                if (hasWithin) {
                    for (int i = 2; i <= this.group; i++) {
                        if (matcher.start(i) > matcher.end(i - 1) + this.within.get(i)) {
                            return false;
                        }
                    }
                }
                if (hasDepth) {
                    for (int i = 2; i <= this.group; i++) {
                        if (matcher.end(i) > this.depth.get(i)) {
                            return false;
                        }
                    }
                }
                return true;
            }
        }
        return false;
    }

    public PacketDataValidator or(PacketDataValidator other) {
        return new CombinedORPatternEngine(this, other);
    }

    private enum Operation {
        PATTERN, DISTANCE, WITHIN, DEPTH;
    }

    private class CombinedORPatternEngine extends PacketDataValidator {

        private PacketDataValidator engineX;
        private PacketDataValidator engineY;

        public CombinedORPatternEngine(PacketDataValidator engineX, PacketDataValidator engineY) {
            super(attribute);
            if (engineX.complexity() < engineY.complexity()) {
                this.engineX = engineX;
                this.engineY = engineY;
            } else {
                this.engineX = engineY;
                this.engineY = engineX;
            }
        }

        @Override
        public boolean test(Event event) {
            return this.engineX.test(event) || this.engineY.test(event);
        }

        @Override
        public void start() {
            this.engineX.start();
            this.engineY.start();
        }
    }
}
