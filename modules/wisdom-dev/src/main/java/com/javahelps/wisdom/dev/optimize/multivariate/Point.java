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

public class Point {

    private final int dimension;
    private double[] coordinates;
    private Constraint[] constraints;

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
