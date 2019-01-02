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

import org.junit.Assert;
import org.junit.Test;

import java.util.function.Function;

public class MultivariateOptimizerTest {

    @Test
    public void testOptimizer1() {
        Function<Point, Double> function = point -> {
            double x = point.getCoordinates()[0];
            double y = point.getCoordinates()[1];

            return x <= 5 && y >= 100 && y <= 1000 ? 0D : 100D;
        };

        Constraint[] locationBounds = {new Constraint(1, 1000), new Constraint(1, 2000)};
        MultivariateOptimizer optimizer = new MultivariateOptimizer(function, locationBounds, new double[]{1, 1});
        Point point = optimizer.execute();
        long x = Math.round(point.getCoordinates()[0]);
        long y = Math.round(point.getCoordinates()[1]);
        Assert.assertTrue("Invalid x value", x >= 1 && x <= 5);
        Assert.assertTrue("Invalid y value", y >= 100 && y <= 1000);
    }

    @Test
    public void testOptimizer2() {
        Function<Point, Double> function = point -> {
            double x = point.getCoordinates()[0];
            double y = point.getCoordinates()[1];

            return Math.pow(2.8125 - x + x * Math.pow(y, 4), 2) +
                    Math.pow(2.25 - x + x * Math.pow(y, 2), 2) +
                    Math.pow(1.5 - x + x * y, 2);
        };

        Constraint[] locationBounds = {new Constraint(1, 4), new Constraint(-1, 1)};
        MultivariateOptimizer optimizer = new MultivariateOptimizer(function, locationBounds, new double[]{1, 1});
        Point point = optimizer.execute();
        Assert.assertEquals("Invalid x value", 3D, point.getCoordinates()[0], 1);
        Assert.assertEquals("Invalid y value", 0.5D, point.getCoordinates()[1], 1);
    }
}
