package com.github.p3spark.io;

import com.github.p3spark.startup.CreateSparkSession;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import javax.xml.crypto.Data;

import static org.junit.jupiter.api.Assertions.*;

class DatabaseTest {

    Database database;
    SparkSession session;
    @BeforeEach
    void setUp() {
        CreateSparkSession startSession = CreateSparkSession.getInstance();
        session = startSession.getSession();
        database = new Database();
    }

    @AfterEach
    void tearDown() {
    }

    @Test
    @Disabled
    void writeToDatabase() {
        //database.writeToDatabase(dataset,1);
    }
}