package com.javahelps.wisdom.manager.util;

import com.javahelps.wisdom.manager.optmize.multivariate.Constraint;

public class Utility {
    public static int getMinPos(double[] list) {
        int pos = 0;
        double minValue = list[0];

        for (int i = 1; i < list.length; i++) {
            if (list[i] < minValue) {
                pos = i;
                minValue = list[i];
            }
        }

        return pos;
    }

    public static Constraint[] velocityBound(Constraint... bounds) {
        int length = bounds.length;
        Constraint[] velocityBounds = new Constraint[length];
        for (int i = 0; i < length; i++) {
            double max = Math.abs(bounds[i].getHigh() - bounds[i].getLow());
            double min = -max;
            velocityBounds[i] = new Constraint(min, max);
        }
        return velocityBounds;
    }
}
