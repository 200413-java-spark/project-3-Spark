package com.github.p3spark.startup;

import org.junit.jupiter.api.*;

class ConsumerTest {
    Consumer consumer;


    @BeforeEach
    void setUp() {
        Consumer consumer = new Consumer();
    }

    @AfterEach
    void tearDown() {
    }

    @Test
    @Disabled
    @DisplayName("Testing builder method")
    void builder() {
        consumer.builder();
//        test technically works but the streaming context is a infinite loop so I don't know how to test something like that yet
    }
}