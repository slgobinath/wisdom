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
