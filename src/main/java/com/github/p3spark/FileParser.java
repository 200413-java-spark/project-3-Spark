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
            parseCSV("OaGAP.csv");
        } catch (IOException e) {
            e.printStackTrace();
        }
        
    }

    private static void parseCSV(String fileName) throws IOException {
        BufferedWriter bw = new BufferedWriter(new FileWriter("OaGAP3.csv"));
        BufferedReader br = new BufferedReader(new FileReader(path + fileName));

        br.readLine(); //This is the headers. Don't 
            String inputLine;
            String inputLine2;
            
            while ((inputLine = br.readLine()) != null) 
            {
                br.mark(1000);
                inputLine2 = br.readLine();
                // System.out.println("Iteration " + i + " inputLine = " + inputLine);
                // System.out.println("inputLine2 = " + inputLine2);

                if(inputLine2.startsWith("("))
                {
                    bw.append(inputLine + " " + inputLine2 + "\n");
                }
                else
                {
                    br.reset();
                }
            }
        bw.close();
        br.close();
      }

     /* 3.10091E+13,Cattaraugus,Iroquois Gas Corp.,13423,0,0,GD,Lakeshore,IN,Milks r47,Persia,Medina,0,0,,,2001,"Persia, NY
      (42.384131, -78.936278)"
      3.10091E+13,Cattaraugus,Iroquois Gas Corp.,13424,0,0,DH,Lakeshore,UN,Samuelson r49,Persia,Medina,0,0,,,2001,"Persia, NY
      (42.384131, -78.936278)"
      3.10091E+13,Cattaraugus,Iroquois Gas Corp.,13427,0,0,OD,Not Applicable,IN,L.Angore (Bridges),UNK,Not Applicable,0,0,,,2001,
      3.10091E+13,Cattaraugus,Iroquois Gas Corp.,13428,0,0,OD,Not Applicable,IN,E.Blackman,UNK,Not Applicable,0,0,,,2001,
      3.10091E+13,Cattaraugus,SWEPI LP,13522,0,0,OD,Bradford,PA,Enterprise Transit St 56,Allegany,"Chipmunk, Bradford 2nd & 3rd",12,0,0,129,2001,"Allegany, NY
      (42.088061, -78.491258)"*/
/*Algorithm: properly parsing input
      First line read
      Second line read
      If second line starts with (, then write it to csv
      If it does NOT, then don't write it and start over
      






*/
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