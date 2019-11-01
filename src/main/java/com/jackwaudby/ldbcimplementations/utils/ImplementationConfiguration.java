package com.jackwaudby.ldbcimplementations.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Reads in configuration properties
 */
public class ImplementationConfiguration {

    private Properties implementationProperties = new Properties();

    /**
     * Load in properties file.
     */
    public ImplementationConfiguration() {
        try {
            String configurationPath = "implementation-configuration.properties";
            implementationProperties.load(new FileInputStream(configurationPath));
        } catch (IOException e) {
            e.printStackTrace(); // TODO: Add error to logger
        }
    }

    /**
     * Get transaction retry attempts.
     * @return transaction attempts
     */
    public Integer getTxnAttempts() {
        return Integer.parseInt(implementationProperties.getProperty("txn.attempts"));
    }
}
