package com.github.p3spark.startup;

import org.junit.jupiter.api.*;

class ConsumerTest {
    Consumer consumer;


    @BeforeEach
    void setUp() {
         consumer = new Consumer();
    }

    @AfterEach
    void tearDown() {
    }

    @Test
    @DisplayName("Testing builder method")
    @Disabled
    void builder() {
        consumer.builder();
//        test technically works but the streaming context is a infinite loop so I don't know how to test something like that yet
    }
}