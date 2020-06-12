package com.github.p3spark.startup;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.*;

class ConsumerTest {
    Consumer consumer;
    SparkSession session;

    @BeforeEach
    void setUp() {
         session = CreateSparkSession.getInstance().getSession();
         consumer = new Consumer();
    }

    @AfterEach
    void tearDown() {
    }

    @Test
    @DisplayName("Testing builder method")
    @Disabled
    void builder() {
        consumer.builder(session);
//        test technically works but the streaming context is a infinite loop so I don't know how to test something like that yet
    }
}