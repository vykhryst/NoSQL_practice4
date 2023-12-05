package org.nosql.vykhryst.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesManager {
    private static final Properties properties;

    static {
        properties = loadProperties();
    }

    private PropertiesManager() {
    }

    private static Properties loadProperties() {
        Properties properties = new Properties();
        String databasePropertiesFile = "database/database.properties";
        try (InputStream input = PropertiesManager.class.getClassLoader().getResourceAsStream(databasePropertiesFile)) {
            if (input == null) {
                throw new RuntimeException("Sorry, unable to find " + databasePropertiesFile);
            }
            properties.load(input);
        } catch (IOException e) {
            throw new RuntimeException("Error loading properties file", e);
        }
        return properties;
    }

    public static String getProperty(String propertyName) {
        return properties.getProperty(propertyName);
    }
}