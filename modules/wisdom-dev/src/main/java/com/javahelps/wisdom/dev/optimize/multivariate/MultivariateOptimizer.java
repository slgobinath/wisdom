package com.javahelps.wisdom.dev.optimize.multivariate;

import com.javahelps.wisdom.dev.util.Constants;
import com.javahelps.wisdom.dev.util.Utility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.function.Function;

public class MultivariateOptimizer {

    private static final Logger LOGGER = LoggerFactory.getLogger(MultivariateOptimizer.class);

    private final Function<Point, Double> function;
    private double globalBestFitness;
    private Point globalBestPoint;
    private final int DIMENSION;
    private final double ERROR_TOLERANCE;
    private final int MAX_ITERATION;
    private final int SWARM_SIZE;
    private final Particle[] swarm;
    private final double[] bestFitness;
    private final double[] fitnessValueList;
    private final Point[] bestPoint;
    private final double PHI_G;
    private final double PHI_P;
    private final double[] steps;

    Random generator = new Random();

    public MultivariateOptimizer(Function<Point, Double> function, Constraint[] locationBounds, double[] steps) {

        this(function, locationBounds, Utility.velocityBound(locationBounds), steps, 100, 0.5, 0.5, 100, 1E-20);
    }

    public MultivariateOptimizer(Function<Point, Double> function, Constraint[] locationBounds, Constraint[] velocityBounds, double[] steps, int swarmSize, double phiG, double phiP, int maxIterations, double errorTolerance) {

        if (locationBounds.length != velocityBounds.length) {
            throw new IllegalArgumentException("Location bounds and Velocity bounds must have same length");
        }
        this.DIMENSION = locationBounds.length;
        this.function = function;
        this.SWARM_SIZE = swarmSize;
        this.MAX_ITERATION = maxIterations;
        this.ERROR_TOLERANCE = errorTolerance;
        this.PHI_G = phiG;
        this.PHI_P = phiP;
        this.steps = steps;
        this.swarm = new Particle[this.SWARM_SIZE];
        this.bestFitness = new double[this.SWARM_SIZE];
        this.fitnessValueList = new double[this.SWARM_SIZE];
        this.bestPoint = new Point[this.SWARM_SIZE];
        for (int i = 0; i < SWARM_SIZE; i++) {
            Particle particle = new Particle(function, new Point(locationBounds), new Velocity(velocityBounds));
            swarm[i] = particle;
            fitnessValueList[i] = particle.getFitnessValue();
            bestFitness[i] = particle.getFitnessValue();
            bestPoint[i] = particle.getLocation();
        }
    }

    public Point execute() {
        // Find an optimal point using Practical Swarm Optimization
        int iteration = 0;
        double error = Double.MAX_VALUE;

        while (iteration < MAX_ITERATION && error > ERROR_TOLERANCE) {

            // Update bestFitness
            for (int i = 0; i < SWARM_SIZE; i++) {
                if (fitnessValueList[i] < bestFitness[i]) {
                    bestFitness[i] = fitnessValueList[i];
                    bestPoint[i] = swarm[i].getLocation();
                }
            }

            // Update globalBestFitness
            int bestParticleIndex = Utility.getMinPos(fitnessValueList);
            if (iteration == 0 || fitnessValueList[bestParticleIndex] < globalBestFitness) {
                globalBestFitness = fitnessValueList[bestParticleIndex];
                globalBestPoint = swarm[bestParticleIndex].getLocation();
            }

            // Update the weight
            double weight = Constants.W_UPPERBOUND - (((double) iteration) / MAX_ITERATION) * (Constants.W_UPPERBOUND - Constants.W_LOWERBOUND);

            // Update velocity and location
            for (int i = 0; i < SWARM_SIZE; i++) {

                double rp = generator.nextDouble();
                double rg = generator.nextDouble();

                Particle particle = swarm[i];
                Point currentLocation = particle.getLocation();

                double[] newVelocity = new double[DIMENSION];
                double[] newLocation = new double[DIMENSION];

                for (int j = 0; j < DIMENSION; j++) {
                    double velocity = weight * particle.getVelocity().getPos()[j] + PHI_P * rp * (currentLocation.getCoordinates()[j] - currentLocation.getCoordinates()[j])
                            + PHI_G * rg * (bestPoint[i].getCoordinates()[j] - currentLocation.getCoordinates()[j]);
                    newVelocity[j] = velocity;
                    newLocation[j] = currentLocation.getCoordinates()[j] + velocity;
                }

                particle.setVelocity(new Velocity(newVelocity));
                Point location = new Point(newLocation, currentLocation.getConstraints());
                particle.setLocation(location);
                fitnessValueList[i] = particle.getFitnessValue();
            }

            error = this.function.apply(globalBestPoint) - 0;

            LOGGER.debug("Iteration {}:\tLoss:{}\tBest values:{}", iteration, this.function.apply(globalBestPoint), globalBestPoint);

            iteration++;
        }
        LOGGER.info("Solution found at iteration {}:\tLoss: {}\tBest values: {}", (iteration - 1), this.function.apply(globalBestPoint), globalBestPoint);

        double[] optimizedPoints = globalBestPoint.getCoordinates();
        Constraint[] constraints = globalBestPoint.getConstraints();
        for (int i = 0; i < DIMENSION; i++) {
            Point point = new Point(globalBestPoint.getCoordinates(), constraints);
            double[] currentPoint = point.getCoordinates();
            double minLimit = constraints[i].getLow();
            double maxLimit = constraints[i].getHigh();
            double newError = error;
            double step = this.steps[i];
            while (newError <= error && currentPoint[i] >= minLimit && currentPoint[i] <= maxLimit) {
                error = newError;
                optimizedPoints[i] = currentPoint[i];
                currentPoint[i] += step;
                newError = this.function.apply(point) - 0;
                iteration++;
            }
        }
        LOGGER.info("Solution found at iteration {}:\tLoss: {}\tBest values: {}", (iteration - 1), this.function.apply(globalBestPoint), globalBestPoint);

        return globalBestPoint;
    }
}
