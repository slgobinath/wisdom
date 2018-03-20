package com.javahelps.wisdom.manager.optimize.multivariate;

class Velocity {
    private double[] velocity;

    public Velocity(double[] velocity) {
        super();
        this.velocity = velocity;
    }

    public Velocity(Constraint... bounds) {
        int dimension = bounds.length;
        this.velocity = new double[dimension];
        for (int i = 0; i < dimension; i++) {
            this.velocity[i] = bounds[i].getRandom();
        }
    }

    public double[] getPos() {
        return velocity;
    }

    public void setPos(double[] vel) {
        this.velocity = vel;
    }

}
