package com.github.p3spark;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
/**
 * This file parses the original csv file into a form that the Dataset<row>.readStream() can accept
 */
public class FileParser {

    
    public static void main(String[] args)
    {
        try {
            parseCSV("OaGAP.csv");
        } catch (IOException e) {
            e.printStackTrace();
        }
        
    }

    private static void parseCSV(String fileName) throws IOException {
        BufferedWriter bw = new BufferedWriter(new FileWriter("OaGAP2.csv"));
        BufferedReader br = new BufferedReader(new FileReader(fileName));
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
    
      

    
}