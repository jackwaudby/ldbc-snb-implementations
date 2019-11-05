package com.jackwaudby.ldbcimplementations.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Reads in configuration properties
 */
public class ImplementationConfiguration {

    private static Properties implementationProperties;


    static{
        try {
            implementationProperties = new Properties();
            String rootPath = Thread.currentThread().getContextClassLoader().getResource("").getPath();
            String configurationPath = rootPath + "implementation-configuration.properties";
            implementationProperties.load(new FileInputStream(configurationPath));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * Get transaction retry attempts.
     * @return transaction attempts
     */
    public static Integer getTxnAttempts() {
        return Integer.parseInt(implementationProperties.getProperty("txn.attempts"));
    }

    /**
     * Get JanusGraph url
     * @return JanusGraph Server url
     */
    public static String getUrl(){
        return implementationProperties.getProperty("url");
    }
}
