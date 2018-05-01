package com.javahelps.wisdom.core.map;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.event.Event;
import com.javahelps.wisdom.core.exception.WisdomAppValidationException;
import com.javahelps.wisdom.core.extension.WisdomExtension;
import com.javahelps.wisdom.core.util.Commons;
import com.javahelps.wisdom.core.variable.Variable;

import java.util.Map;

import static com.javahelps.wisdom.core.util.WisdomConstants.VARIABLE;

@WisdomExtension("variable")
public class VariableMapper extends Mapper implements Variable.OnUpdateListener<Comparable> {

    private final String varName;
    private Variable<Comparable> variable;
    private Comparable value;

    public VariableMapper(String attrName, Map<String, ?> properties) {
        super(attrName, properties);
        this.varName = Commons.getProperty(properties, VARIABLE, 0);
        if (this.varName == null) {
            throw new WisdomAppValidationException("Required property %s for Variable mapper not found", VARIABLE);
        }
    }

    @Override
    public void start() {

    }

    @Override
    public void init(WisdomApp wisdomApp) {
        this.variable = wisdomApp.getVariable(this.varName);
        this.value = this.variable.get();
        this.variable.addOnUpdateListener(this);
    }

    @Override
    public void stop() {
        this.variable.removeOnUpdateListener(this);
    }

    @Override
    public Event apply(Event event) {
        return event.set(this.attrName, this.value);
    }

    @Override
    public void update(Comparable value) {
        this.value = value;
    }
}
