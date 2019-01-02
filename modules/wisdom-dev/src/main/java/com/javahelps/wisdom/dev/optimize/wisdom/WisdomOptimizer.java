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

package com.javahelps.wisdom.dev.optimize.wisdom;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.variable.Variable;
import com.javahelps.wisdom.dev.optimize.multivariate.Constraint;
import com.javahelps.wisdom.dev.optimize.multivariate.MultivariateOptimizer;
import com.javahelps.wisdom.dev.optimize.multivariate.Point;
import com.javahelps.wisdom.dev.util.Constants;
import com.javahelps.wisdom.dev.util.Utility;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


public class WisdomOptimizer {

    private final WisdomApp app;
    private final List<QueryTrainer> queryTrainers = new ArrayList<>();
    private final List<Variable<Long>> variables = new ArrayList<>();
    private final List<Constraint> constraints = new ArrayList<>();
    private final List<Double> steps = new ArrayList<>();
    private final int swarmSize;
    private final int maxIterations;

    public WisdomOptimizer(WisdomApp app) {
        this(app, 100, 100);
    }

    public WisdomOptimizer(WisdomApp app, int swarmSize, int maxIterations) {
        this.app = app;
        this.swarmSize = swarmSize;
        this.maxIterations = maxIterations;
        List<Variable> trainable = app.getTrainable();
        for (Variable variable : trainable) {
            Properties properties = variable.getProperties();
            Number min = (Number) properties.get(Constants.MINIMUM);
            Number max = (Number) properties.get(Constants.MAXIMUM);
            Number step = (Number) properties.get(Constants.STEP);
            Constraint constraint = new Constraint(min.doubleValue(), max.doubleValue());
            this.addTrainable(variable.getId(), constraint, step.doubleValue());
        }
    }

    public void addTrainable(String varName, Constraint constraint, double step) {

        Variable variable = this.app.getVariable(varName);
        if (variable == null) {
            throw new IllegalArgumentException(String.format("Variable id %s does not exist", varName));
        }
        this.variables.add(variable);
        this.constraints.add(constraint);
        this.steps.add(step);
    }

    public void addQueryTrainer(QueryTrainer queryTrainer) {

        this.queryTrainers.add(queryTrainer);
        queryTrainer.init(this.app);
    }

    private void updateVariables(Point point) {

        final int noOfVariables = this.variables.size();
        double[] values = point.getCoordinates();
        for (int i = 0; i < noOfVariables; i++) {
            long value = (long) values[i];
            this.variables.get(i).set(value);
        }
    }

    private Double objectiveFunction(Point point) {

        this.app.clear();
        this.updateVariables(point);
        double loss = 0.0;
        for (QueryTrainer trainer : this.queryTrainers) {
            trainer.train();
            loss += trainer.loss();
        }
        return loss;
    }


    public Point optimize() {

        int noOfVariables = this.variables.size();
        Constraint[] constraints = this.constraints.toArray(new Constraint[0]);
        double[] steps = new double[noOfVariables];
        for (int i = 0; i < noOfVariables; i++) {
            steps[i] = this.steps.get(i);
        }
        MultivariateOptimizer optimizer = new MultivariateOptimizer(this::objectiveFunction, constraints, Utility.velocityBound(constraints), steps, swarmSize, 0.5, 0.5, maxIterations, 1E-20);
        Point point = optimizer.execute();
        return point;
    }
}
