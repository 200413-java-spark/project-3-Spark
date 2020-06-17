package com.github.p3spark.startup;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

class ConsumerTest {
    CreateSparkSession startSession;
    SparkSession session;
    Consumer consumer;

    @BeforeEach
    void setUp() {
        startSession = CreateSparkSession.getInstance();
        session = startSession.getSession();
    }

    @Test
    @Disabled
    @DisplayName("Builder works but runs in a infinite loop")
    void builder() {
        consumer.builder(session);
    }
}