package com.jackwaudby.ldbcimplementations.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Reads in configuration properties
 */
public class ImplementationConfiguration {

    private String configurationPath = "implementation-configuration.properties";
    private Properties implementationProperties = new Properties();

    public ImplementationConfiguration() {
        try {
            implementationProperties.load(new FileInputStream(configurationPath));
        } catch (IOException e) {
            e.printStackTrace(); // TODO: Add error to logger
        }
    }

    public Integer getTxnAttempts() {
        return Integer.parseInt(implementationProperties.getProperty("txn.attempts"));
    }
}
