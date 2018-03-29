package com.javahelps.wisdom.service.util;

import java.io.File;
import java.io.IOException;

/**
 * Created by gobinath on 7/10/17.
 */
public class TestUtil {

    public static void execTestServer(long waitingTime) {
        String javaHome = System.getProperty("java.home");
        String javaBin = javaHome +
                File.separator + "bin" +
                File.separator + "java";
        String classpath = System.getProperty("java.class.path");
        String className = TestServer.class.getCanonicalName();

        ProcessBuilder builder = new ProcessBuilder(javaBin, "-cp", classpath, className, Long.toString(waitingTime));

        try {
            builder.start();
        } catch (IOException e) {
            throw new RuntimeException("Failed to execute the class in separate JVM", e);
        }
    }
}
