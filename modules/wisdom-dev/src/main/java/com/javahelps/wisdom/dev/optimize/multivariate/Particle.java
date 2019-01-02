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
