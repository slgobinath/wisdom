package com.javahelps.wisdom.manager.optimize.multivariate;

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
        Assert.assertEquals("Invalid x value", 5D, point.getCoordinates()[0], 1);
        Assert.assertEquals("Invalid y value", 1000D, point.getCoordinates()[1], 1);
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
