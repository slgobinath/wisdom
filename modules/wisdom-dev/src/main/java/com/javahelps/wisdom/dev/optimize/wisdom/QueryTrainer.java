package com.javahelps.wisdom.dev.optimize.wisdom;

import com.javahelps.wisdom.core.WisdomApp;

public interface QueryTrainer {

    void init(WisdomApp app);

    void train();

    double loss();
}
