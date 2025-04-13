package com.epam.spark;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Properties;

public class PropertiesService implements Serializable {

    private final Properties properties = new Properties();

    public PropertiesService() {
        try (InputStream fileInputStream = getClass().getClassLoader().getResourceAsStream("conf.properties")) {
            properties.load(fileInputStream);
        } catch (IOException e) {
            throw new RuntimeException("Cannot load conf.properties", e);
        }
    }

    public String getProperty(String key) {
        return properties.getProperty(key);
    }
}
