package com.jackwaudby.ldbcimplementations.utils;

import org.apache.commons.configuration.Configuration;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

/**
 * Reads in configuration properties
 */
public class ImplementationConfiguration {

    private static org.apache.log4j.Logger LOGGER = org.apache.log4j.Logger.getLogger(ImplementationConfiguration.class.getName());

    private static Properties implementationProperties = new Properties();


    static {
        try {
            InputStream in = Configuration.class.getResourceAsStream("/implementation-configuration.properties");
            implementationProperties.load(in);
        } catch (Exception e) {
            LOGGER.error(e);
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
