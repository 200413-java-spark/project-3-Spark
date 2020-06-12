package com.github.p3spark.utils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ConfigPropertiesTest {

    ConfigProperties configProperties;

    @BeforeEach
    void setUp() {
        configProperties = new ConfigProperties();
    }

    @Test
    void getDriver() {
        configProperties.getDriver();
    }

    @Test
    void getDbtable() {
        configProperties.getDbtable();
    }

    @Test
    void getUser() {
        configProperties.getUser();
    }

    @Test
    void getPassword() {
        configProperties.getPassword();
    }
}