package com.teksystems.paagarwa;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;


import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

//You need to import the java.sql package to use JDBC
import java.sql.*;

//You need to import oracle.jdbc.driver.* in order to use the
//API extensions.
import oracle.jdbc.driver.*;


public class JsonReader{
	ObjectMapper jsonMapper = new ObjectMapper();
    JsonFactory jsonFactory = new JsonFactory();
	File file;
	int fType;
	String[][] elements;
	Connection conn ;
	public JsonReader(String file ) throws SQLException {
		// Load the Oracle JDBC driver
	    DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
		// Connect to the database
	    // You can put a database name after the @ sign in the connection URL.
	    conn =
	      DriverManager.getConnection ("jdbc:oracle:thin:@localhost:1521/exavm03", "wgp", "wgp");
	    System.out.println("connection " + conn.isValid(10));
	    ((OracleConnection)conn).setDefaultExecuteBatch (10);
	    
	    JsonNode jn;
	     try {
				InputStream in = new FileInputStream(file);
				JsonParser jsonParser = jsonFactory.createParser(in);
		        parseObject(jsonParser);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	}
    

    
 public static void main(String[] args) throws SQLException {
	

	    
	    String file = args[0];
	    //System.out.println("filepath " + file);
	    long start = System.nanoTime();
        try {
            new JsonReader(file);
        } catch (Exception e) {
            e.printStackTrace();
        }
        long end = System.nanoTime();
        System.out.println("Time taken (nano seconds): " + (end - start));
        //conn.close();
    }

 public void parseObject(JsonParser json) throws JsonProcessingException, IOException, SQLException  {
     JsonFactory factory = new JsonFactory();

     ObjectMapper mapper = new ObjectMapper(factory);
     JsonNode rootNode = mapper.readTree(json);  
     parseNode(rootNode,null,null);
 }
 
 public void  parseNode(JsonNode json,  String table, PreparedStatement ps) throws JsonProcessingException, IOException, SQLException  {
     JsonNode rootNode = json;
     int tbl = 2;
     //String space = String.format("%"+ numberOfSpaces +"s", " ");
     Iterator<Map.Entry<String,JsonNode>> fieldsIterator = rootNode.fields();
     while (fieldsIterator.hasNext()) {
    	 
         Map.Entry<String,JsonNode> field = fieldsIterator.next();     
         if(field.getValue().isObject()){	
        	 ConcurrentLinkedQueue<String> rec = new ConcurrentLinkedQueue<String>();
        	 parseNode(field.getValue(),field.getKey(),ps);
        	 //System.out.println("Total att: " + rec.size());
        	 String ques="";
        	 for(int i=0;i<rec.size();i++)
        		 ques += ",?";
        	 //System.out.println("Total ques " + ques);
        	 String ins = "INSERT INTO " + field.getKey() + " VALUES(?" + ques + ")";
        	// System.out.println(ins);
        	/* PreparedStatement ps =
       		      conn.prepareStatement (ins); 
        	 ps.setInt(1, 1);*/
        	 int n = rec.size();
        	 for(int i=0; i < n; i++){
        		 String tmp = rec.remove();
        		 //System.out.println("att: " + tmp);
        		// ps.setString(i+2, tmp);
        		 
        	 }
        	 	/*int rows = ps.executeUpdate ();
        	 	ps.close();*/
        	    //System.out.println ("Number of rows updated now: " + rows);  
         }else if(field.getValue().isArray()){
        	 String tblName = field.getKey();   
        	 //System.out.println("array");
        	 String ques="";
        	 for(int i =1; i<39;i++)
        		 ques+=",?";
        	 String ins = "INSERT INTO RecQc VALUES(?" + ques + ")";
    		 System.out.println(ins);
    		 ConcurrentLinkedQueue<String> rec = new ConcurrentLinkedQueue<String>();
        	 
        	 PreparedStatement ps1 =
          		      conn.prepareStatement (ins);
    		 //ps.setInt(1,1);
        	 for (final JsonNode objNode : field.getValue()) {
        		 ps1.setInt(1,1);
        		 tbl =2;
        		 parseNode(objNode, field.getKey(),ps1);
        		 ps1.executeUpdate();
        		 System.out.println();
        		 //break;
            	 /*for(int j =0; j <8; j++)
            	 	ps.setInt(j+1,1);
            		
               		ps.setString(9, rec.remove());
               		ps.setString(10, rec.remove());
               		int rows = ps.executeUpdate ();
               		ps.close();*/
               	    //System.out.println ("Number of rows updated now: " + rows);  
        		// break;
        	 }
        	 
         }else{	 
        	 if(table == "RecQc"){
        		 tbl++;
        		 String st = field.getValue().toString();
        		 System.out.print(tbl + ": " + field.getKey() + ": " + st + "\t");
        		 ps.setString(tbl, st);
        	 //records.add(field.getValue().toString());
        	 }
         }
     }
 }

}