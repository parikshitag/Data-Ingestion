package com.teksystems.paagarwa;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
//You need to import the java.sql package to use JDBC
//You need to import oracle.jdbc.driver.* in order to use the
//API extensions.


public class JsonReader{
	ObjectMapper jsonMapper = new ObjectMapper();
    JsonFactory jsonFactory = new JsonFactory();
	File file;
	int fType;
	String[][] elements;
	Connection conn ;
    

    
 public static void main(String[] args) throws SQLException {
	// Load the Oracle JDBC driver
	    DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());

	    String file = args[0];
	    //System.out.println("filepath " + file);
	    long start = System.nanoTime();
        try {
            new JsonReader().getNames(file);
        } catch (Exception e) {
            e.printStackTrace();
        }
        long end = System.nanoTime();
        System.out.println("Time taken (nano seconds): " + (end - start));
    }
 void getNames(String file) throws Exception {

     try {
			InputStream in = new FileInputStream(file);
			JsonParser jsonParser = jsonFactory.createParser(in);
	        parseObject(jsonParser);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
 }
 public void parseObject(JsonParser json) throws JsonProcessingException, IOException, SQLException  {
     JsonFactory factory = new JsonFactory();

     ObjectMapper mapper = new ObjectMapper(factory);
     JsonNode rootNode = mapper.readTree(json);  
     parseNode(rootNode,1);
 }
 
 public void  parseNode(JsonNode json, int numberOfSpaces) throws JsonProcessingException, IOException, SQLException  {
     JsonNode rootNode = json;
     
     String space = String.format("%"+ numberOfSpaces +"s", " ");
     Iterator<Map.Entry<String,JsonNode>> fieldsIterator = rootNode.fields();
     while (fieldsIterator.hasNext()) {
    	 
         Map.Entry<String,JsonNode> field = fieldsIterator.next();     
         if(field.getValue().isObject()){	
        	 
        	 System.out.println(field.getKey());
        	 parseNode(field.getValue(),numberOfSpaces);
        	 //System.out.println("Total att: " + rec.size());
        	 
        	 
        	    //System.out.println ("Number of rows updated now: " + rows);  
         }else if(field.getValue().isArray()){
        	 String tblName = field.getKey();   
        	 System.out.println(tblName);
        	 for (final JsonNode objNode : field.getValue()) {
        		 String ins = "INSERT INTO " + tblName + " VALUES(?,?,?,?,?,?,?,?,?,?)";
        		 
        		 numberOfSpaces+=5;
            	 parseNode(objNode,numberOfSpaces);
            	 numberOfSpaces-=5;
               	    //System.out.println ("Number of rows updated now: " + rows);  
        		 break;
        	 }
        	 
         }else{	 
        	 System.out.println(space + field.getKey());
         }
     }
 }
}

