package com.javahelps.wisdom.manager.optimize.wisdom;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.variable.Variable;
import com.javahelps.wisdom.manager.optimize.multivariate.Constraint;
import com.javahelps.wisdom.manager.optimize.multivariate.MultivariateOptimizer;
import com.javahelps.wisdom.manager.optimize.multivariate.Point;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;

import static com.javahelps.wisdom.manager.util.Constants.*;

public class WisdomOptimizer {

    private final WisdomApp app;
    private final List<QueryTrainer> queryTrainers = new ArrayList<>();
    private final List<Variable<Long>> variables = new ArrayList<>();
    private final List<Constraint> constraints = new ArrayList<>();
    private final List<Double> steps = new ArrayList<>();

    public WisdomOptimizer(WisdomApp app) {
        this.app = app;
        List<Variable> trainable = app.getTrainable();
        for (Variable variable : trainable) {
            Properties properties = variable.getProperties();
            Number min = (Number) properties.get(MINIMUM);
            Number max = (Number) properties.get(MAXIMUM);
            Number step = (Number) properties.get(STEP);
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
        MultivariateOptimizer optimizer = new MultivariateOptimizer(this::objectiveFunction, constraints, steps);
        Point point = optimizer.execute();
        return point;
    }

    public static Callable<Point> callable(WisdomApp app, QueryTrainer... trainers) {
        WisdomOptimizer optimizer = new WisdomOptimizer(app);
        for (QueryTrainer trainer : trainers) {
            optimizer.addQueryTrainer(trainer);
        }
        Callable<Point> callable = () -> {
            app.start();
            Point point = optimizer.optimize();
            app.shutdown();
            return point;
        };
        return callable;
    }
}
