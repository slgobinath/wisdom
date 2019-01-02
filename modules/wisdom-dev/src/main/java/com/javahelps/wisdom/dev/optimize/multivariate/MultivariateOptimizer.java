/*
 * Copyright (c) 2018, Gobinath Loganathan (http://github.com/slgobinath) All Rights Reserved.
 *
 * Gobinath licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. In addition, if you are using
 * this file in your research work, you are required to cite
 * WISDOM as mentioned at https://github.com/slgobinath/wisdom.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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
    private double globalBestFitness;
    private Point globalBestPoint;

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

            error = this.function.apply(globalBestPoint);

            LOGGER.debug("Iteration {}:\tLoss:{}\tBest values:{}", iteration, this.function.apply(globalBestPoint), globalBestPoint);

            iteration++;
        }
        iteration--;
        LOGGER.info("Solution found at iteration {}:\tLoss: {}\tBest values: {}", iteration, this.function.apply(globalBestPoint), globalBestPoint);

        double[] optimizedPoints = globalBestPoint.getCoordinates();
        Constraint[] constraints = globalBestPoint.getConstraints();
        int maxIteration = MAX_ITERATION + 50;
        for (int i = 0; i < DIMENSION; i++) {
            Point point = new Point(globalBestPoint.getCoordinates(), constraints);
            double[] currentPoint = point.getCoordinates();
            double direction = this.steps[i];
            double boundary;
            if (direction == 0.0) {
                continue;
            } else if (direction > 0) {
                boundary = constraints[i].getHigh();
            } else {
                boundary = constraints[i].getLow();
            }
            while (iteration <= maxIteration && error > ERROR_TOLERANCE) {

                double step = (boundary - currentPoint[i]) / 2;
                if (Math.abs(step) < Math.abs(direction)) {
                    step = direction;
                }
                // Increase the value
                currentPoint[i] += step;
                if (Math.abs(currentPoint[i] - boundary) < 1) {
                    currentPoint[i] = optimizedPoints[i];
                    break;
                }
                double newError = this.function.apply(point);
                if (newError > error) {
                    boundary = currentPoint[i];
                    currentPoint[i] = optimizedPoints[i];
                } else {
                    error = newError;
                    optimizedPoints[i] = currentPoint[i];
                }
                LOGGER.debug("Iteration {}:\tLoss:{}\tBest values:{}", iteration, error, globalBestPoint);
                iteration++;
            }
        }
        LOGGER.info("Solution found at iteration {}:\tLoss: {}\tBest values: {}", (iteration - 1), this.function.apply(globalBestPoint), globalBestPoint);

        return globalBestPoint;
    }
}
