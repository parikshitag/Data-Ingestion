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
import java.util.UUID;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
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
	int num=2;
	public JsonReader(String file) throws SQLException {
		// Load the Oracle JDBC driver
	    DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
	    // Connect to the database
	    // You can put a database name after the @ sign in the connection URL.
	    conn =
	      DriverManager.getConnection ("jdbc:oracle:thin:@localhost:1521/exavm03", "wgp", "wgp");
	    System.out.println("connection " + conn.isValid(10));
	    long start = System.nanoTime();
	    ((OracleConnection)conn).setDefaultExecuteBatch(1);
	    try {
			InputStream in = new FileInputStream(file);
			JsonParser jsonParser = jsonFactory.createParser(in);
	        parseObject(jsonParser);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    long end = System.nanoTime();
        System.out.println("Time taken (nano seconds): " + (end - start));
        conn.close();
	}
    


 public void parseObject(JsonParser json) throws JsonProcessingException, IOException, SQLException  {
     JsonFactory factory = new JsonFactory();
     ObjectMapper mapper = new ObjectMapper(factory);
     JsonNode rootNode = mapper.readTree(json);  
     String ques="?";
	 for(int i=0;i<30;i++)
		 ques += ",?";
     String ins = "INSERT INTO MASTER_META_DATA VALUES(" + ques + ")";
 	 System.out.println(ins);
 	 PreparedStatement ps =
		      conn.prepareStatement (ins);
 	 ps.setString(1, UUID.randomUUID().toString());
     parseNode(rootNode,ps);
     ps.close();
 }
 
 public void  parseNode(JsonNode json,PreparedStatement ps) throws JsonProcessingException, IOException, SQLException  {
     JsonNode rootNode = json;
     
     Iterator<Map.Entry<String,JsonNode>> fieldsIterator = rootNode.fields();
     while (fieldsIterator.hasNext()) {
    	 
         Map.Entry<String,JsonNode> field = fieldsIterator.next();     
         if(field.getValue().isObject()){	
        	 
        	 //System.out.println(field.getKey());
        	 parseNode(field.getValue(),ps);
        	 //System.out.println("Total att: " + rec.size());
        	 
        	 
        	    //System.out.println ("Number of rows updated now: " + rows);  
         }else if(field.getValue().isArray()){
        	 /*String tblName = field.getKey();   
        	 //System.out.println(tblName);
        	 for (final JsonNode objNode : field.getValue()) {
        		 String ins = "INSERT INTO " + tblName + " VALUES(?,?,?,?,?,?,?,?,?,?)";
        		 
        		 numberOfSpaces+=5;
            	 parseNode(objNode,numberOfSpaces);
            	 numberOfSpaces-=5;
               	    //System.out.println ("Number of rows updated now: " + rows);  
        		 break;
        	 }
        	 */
         }else{	 
        	 if(field.getValue().isInt() || field.getValue().isDouble()){
        		 System.out.println(num + " " + field.getKey() +  " NUMBER, " + field.getValue());
        		 ps.setInt(num++, field.getValue().asInt());
        	 }else if(field.getValue().isBoolean()){
        		 System.out.println(num + " bool " + field.getValue());
        		 ps.setBoolean(num++, field.getValue().asBoolean());
        	 }else{
        		 System.out.println(num + " string " + field.getValue());
        		 ps.setString(num++, field.getValue().asText());
        	 }
         }
     }
 }
 
 public static void main(String[] args) throws SQLException {
     try {
         new JsonReader(args[0]);
     } catch (Exception e) {
         e.printStackTrace();
     }
     
}
}

