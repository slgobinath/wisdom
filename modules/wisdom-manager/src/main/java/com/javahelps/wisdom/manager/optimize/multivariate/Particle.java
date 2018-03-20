package com.javahelps.wisdom.manager.optimize.multivariate;

import java.util.function.Function;

class Particle {

    private final Function<Point, Double> function;
    private double fitnessValue;
    private Velocity velocity;
    private Point location;

    public Particle(Function<Point, Double> function) {
        this.function = function;
    }

    public Particle(Function<Point, Double> function, Point location, Velocity velocity) {
        this(function);
        this.location = location;
        this.velocity = velocity;
    }

    public Velocity getVelocity() {
        return velocity;
    }

    public void setVelocity(Velocity velocity) {
        this.velocity = velocity;
    }

    public Point getLocation() {
        return location;
    }

    public void setLocation(Point location) {
        this.location = location;
    }

    public double getFitnessValue() {
        fitnessValue = this.function.apply(location);
        return fitnessValue;
    }
}
