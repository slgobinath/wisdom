package com.javahelps.wisdom.manager.util;

public class Constants {

    private Constants() {

    }

    public static final String ARTIFACTS_DIR = "artifacts";

    public static final String CONF_DIR = "conf";

    public static final String ARTIFACTS_CONFIG_FILE = "artifacts.yaml";

    public static final String WISDOM_HOME = "WISDOM_HOME";

    public static final int HTTP_CREATED = 201;

    public static final int MINIMUM_PRIORITY_THRESHOLD = 7;

    public static final double MINIMUM_THROUGHPUT_THRESHOLD = 0.0;

    public static final double MAXIMUM_THROUGHPUT_THRESHOLD = 0.00000001;

    public static final int THROUGHPUT_WINDOW_LENGTH = 10;

    public static long STATISTIC_MONITOR_INITIAL_DELAY = 900000L;   // 15 minutes

    public static long STATISTIC_MONITOR_PERIOD = 900000L;   // 15 minutes
}
