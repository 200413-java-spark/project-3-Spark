package com.github.p3spark;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
/**
 * This file parses the original csv file into a form that the Dataset<row>.readStream() can accept
 */
public class FileParser {

    private static String path = "C:\\Users\\Garrison\\project-3-Spark\\project-3-Spark\\";
    
    public static void main(String[] args)
    {
        System.out.println("out");
        try {
            
            parseNewGeoReferenceColumn();
        } catch (IOException e) {
            e.printStackTrace();
        }
        
    }

    private static void parseTwoLinesToOne(String fileName) throws IOException {
        BufferedWriter bw = new BufferedWriter(new FileWriter("OaGAP2.csv"));
        BufferedReader br = new BufferedReader(new FileReader(path + fileName));
        br.readLine(); //This is the headers. Don't 
            String inputLine;
            String inputLine2;
            while ((inputLine = br.readLine()) != null) 
            {
                inputLine2 = br.readLine();
                bw.append(inputLine + " " + inputLine2 + "\n");
            }
        bw.close();
        br.close();
      }

      private static void parseNewGeoReferenceColumn() throws IOException
      {
        BufferedWriter bw = new BufferedWriter(new FileWriter(path + "results3.csv"));
        BufferedReader br = new BufferedReader(new FileReader(path + "OaGAP2.csv"));
            br.readLine(); //This is the headers. Don't 
            String inputLine;
            String outputLine;

            while ((inputLine = br.readLine()) != null) 
            {
                outputLine = inputLine.replace("(",",").replace(")","");
                bw.append(outputLine + "\n");
            }

        bw.close();
        br.close();
          
      }
      
      public static void turnIntoCSV(Dataset<Row> dataRow, String fileName) throws IOException {

        BufferedWriter bw = new BufferedWriter(new FileWriter(fileName));
        String columnList = String.join(",", dataRow.columns()).replace("(","").replace(")","");
        
        bw.append(columnList + "\n");
    
        for (Row row : dataRow.collectAsList()) 
        {
          bw.append(row.toString().replace("[","").replace("]","") + "\n");
        }
    
        bw.close();
      }
    

    
}