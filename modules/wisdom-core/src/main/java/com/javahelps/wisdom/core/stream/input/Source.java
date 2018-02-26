package com.javahelps.wisdom.core.stream.input;

import com.javahelps.wisdom.core.WisdomApp;
import com.javahelps.wisdom.core.stream.InputHandler;

public interface Source {

    void init(WisdomApp wisdomApp, String streamId, InputHandler inputHandler);

    void start();

    void stop();
}
