package org.apache.seatunnel.common.utils;

public class EnvUtil {

    public static String getEnv(String key, String defVal) {
        String propVal = System.getProperty(key);
        if (propVal == null) {
            propVal = System.getenv(key);
        }
        return propVal == null ? defVal : propVal;
    }

    public static String getEnv(String key) {
        return getEnv(key, null);
    }
}
