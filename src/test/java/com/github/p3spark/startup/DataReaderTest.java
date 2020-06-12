package com.github.p3spark.startup;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DataReaderTest {

    DataReader dataReader;
    SparkSession session;
    @BeforeEach
    void setUp() {
        CreateSparkSession startSession = CreateSparkSession.getInstance();
        session = startSession.getSession();
        dataReader = new DataReader();
    }

    @AfterEach
    void tearDown() {
    }

    @Test
    void parseHeaders() {
        //dataReader.parseHeaders(dataset);
    }
}