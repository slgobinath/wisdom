package com.javahelps.wisdom.dev.optimize.multivariate;

import java.util.Random;
import java.util.function.Predicate;

public class Constraint {

    private final double low;
    private final double high;
    private final Predicate<Double> predicate;
    private final Random generator = new Random();

    public Constraint(double low, double high, Predicate<Double> predicate) {
        this.low = low;
        this.high = high;
        this.predicate = predicate;
    }

    public Constraint(double low, double high) {
        this(low, high, x -> true);
    }

    public double getHigh() {
        return high;
    }

    public double getLow() {
        return low;
    }

    private double nextRandom() {
        return this.low + generator.nextDouble() * (this.high - this.low);
    }

    public double getRandom() {
        double value = nextRandom();
        while (!this.predicate.test(value)) {
            value = nextRandom();
        }
        return value;
    }

    public double limit(double currentValue, double newValue) {
        double value;
        if (this.predicate.test(newValue)) {
            value = newValue;
        } else {
            value = currentValue;
        }
        if (value < this.low) {
            value = this.low;
        } else if (value > this.high) {
            value = this.high;
        }
        return value;
    }
}
