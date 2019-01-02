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
