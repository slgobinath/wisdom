package com.javahelps.wisdom.core.time;

/**
 * Created by gobinath on 7/10/17.
 */
@FunctionalInterface
public interface Executor {

    void execute(long timestamp) throws Exception;
}
