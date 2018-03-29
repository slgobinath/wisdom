package com.javahelps.wisdom.dev.optimize.multivariate;

public class Point {

    private double[] coordinates;
    private Constraint[] constraints;
    private final int dimension;

    public Point(Constraint... constraints) {
        this.dimension = constraints.length;
        this.constraints = constraints;
        this.coordinates = new double[dimension];
        for (int i = 0; i < this.dimension; i++) {
            this.coordinates[i] = constraints[i].getRandom();
        }
    }

    public Point(double[] coordinates, Constraint... constraints) {
        this.dimension = constraints.length;
        this.constraints = constraints;
        this.coordinates = new double[dimension];
        for (int i = 0; i < this.dimension; i++) {
            this.coordinates[i] = this.constraints[i].limit(this.coordinates[i], coordinates[i]);
        }
    }

    public double[] getCoordinates() {
        return coordinates;
    }

    public Constraint[] getConstraints() {
        return constraints;
    }

    @Override
    public String toString() {
        int iMax = dimension - 1;

        StringBuilder b = new StringBuilder();
        b.append('[');
        for (int i = 0; ; i++) {
            b.append(String.format("%.2f", coordinates[i]));
            if (i == iMax)
                return b.append(']').toString();
            b.append(", ");
        }
    }
}
