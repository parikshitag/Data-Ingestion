package com.teksystems.paagarwa;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
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
	final String hash = UUID.randomUUID().toString();
	Connection conn;
	//int index1=2;		//array elements	
	//int index2=2;		//not array
	final List<String> index1 = Arrays.asList("ReceiverLineNumber","ReceiverPointNumber","ReceiverPointIndex","ReshootIndex","GroupIndex","DepthIndex","ExtendedTraceNumber","SensorMoving","PhysicalUnit","SensorType","KilledByShotGather","PartiallyComplete","Warpurated","Muted","Ntbp","Overflow","Dead","Uncalibrated","Noisy","Weak","DigitalSpike","GravitySpike","EnergySpike","AmplitudeSpike","ColocatedError","ToBeRestoredSpiky","SampleReplacementMethod","NumberOfReplacedSamples","NormalizedPSD","MedianGravity","NormalizedNoiselevelGravity","NormalizedEnergy","AppliedScalingfFactor","ChannelOneStatus","ChannelTwoStatus","ChannelThreeStatus","ChannelFourStatus","ChannelFiveStatus","ChannelSixStatus","InconsistentlyScaled","HydrophoneDc","RotationQcError","HydrophoneScaledUp","HydrophoneScaledDown","RotationQcWarning","DetectedConnector","HydrophoneDistorted","RegOffsetFromStreamer","NominalDepth","MeasuredDepth","DataQuality","LowFrequencyNoiseWarning","EstimatedDepth","StreamerTwist","DC","CrossoverFrequency","HydrophoneStatusAsInt","CableId","SectionNetworkId","Layer","DsnId","SensorId","DsnSerialNumber","NominalZeroOffset","NominalSensitivity","DistanceFromFront","MaxVolt","ChannelStatus","WarpStatus","SamplePeriod","FilterDelay","FilterPhase","HiCutFrequency","LowCutFrequency","DataFormat","AdcGain","LowCutSlope","HiCutSlope","NominalAmpGainMultiplicationFactor","DcOffsetRemoval","SensorNormalization","SensorCalibration","TsgCalibration","GainCalibration","MisalignCalibration","MeasuredDcOffset","NominalAmplifierGain","InstrumentTestTime","SensorSensitivity","InstrumentTestResult","SerialNumber","ScanTypeNumber","ChannelSetNumber","ChannelTypeHex","ChannelTypeString","MpFactor","SampleSize","StartTime","EndTime","ChannelDesc","StreamerNumber","N6SensorType","SectionId","TraceNumber","TraceEdit");
	final List<String> index2 = Arrays.asList("VesselCode","VesselName","LineAbbreviation","LineName","JobNumber","ShotNumber","Major","Minor","SpecificationMajor","SpecificationMinor","DataSource","DataType","SeismicDataType","StreamerList","NofTraces","externalHeaderData","extendedHeaderData","jsonType","jsonTypeAsString","lineSequenceNumber","shotNo","shotTimeInMillis","shotTimeInUTC","processingVersion","streamerNumber","sailLineName","sequenceType","surveyName");
	
	boolean flag=false;
	public JsonReader(String file) throws SQLException, JsonProcessingException, IOException {
		// Load the Oracle JDBC driver
	    DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
	    // Connect to the database
	    // You can put a database name after the @ sign in the connection URL.
	    conn = DriverManager.getConnection ("jdbc:oracle:thin:@localhost:1521/exavm03", "wgp", "wgp");
	   // System.out.println("connection " + conn.isValid(10));
	    
	    ((OracleConnection)conn).setDefaultExecuteBatch(1000);
			InputStream in = new FileInputStream(file);
			JsonParser jsonParser = jsonFactory.createParser(in);
	       
	    long start = System.nanoTime();
	    
		parseObject(jsonParser);
	    long end = System.nanoTime();
        System.out.println("Time taken (nano seconds): " + (end - start));
        conn.close();
	}
    


 public void parseObject(JsonParser json) throws JsonProcessingException, IOException, SQLException  {
     JsonFactory factory = new JsonFactory();
     ObjectMapper mapper = new ObjectMapper(factory);
     JsonNode rootNode = mapper.readTree(json);  
     String ques="?";
	 for(int i=0;i<28;i++)
		 ques += ",?";
	 String ins = "INSERT INTO MASTER_META_DATA VALUES(" + ques + ")";
	 
 	 PreparedStatement ps =
		      conn.prepareStatement (ins);
 	
 	
 	 ps.setString(1, hash);
     parseNode(rootNode,ps);
     ps.executeUpdate();
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
        	 String ques = "?";
        	 for(int i=0;i<105;i++)	
        		 ques += ",?";     
             String ins = "INSERT INTO SENSOR_RAW_TRACEDATA VALUES(" + ques + ")";
             //System.out.println(ins);
        	 PreparedStatement ps1 =
       		      conn.prepareStatement (ins);
        	 //String tblName = field.getKey();   
        	 //System.out.println(tblName);
        	 for (final JsonNode objNode : field.getValue()) {
        		 ps1.setString(1, hash);
        		 flag=true;
            	 parseNode(objNode,ps1);
            	 flag=false;
            	 //System.out.println("finally here");
            	 ps1.executeUpdate();
               	    //System.out.println ("Number of rows updated now: " + rows);  
        		 //break;
        	 }
        	 
        	 ps1.close();     	 
         }else{	 
        	 int index;
        	 if(flag)
        		 index = index1.indexOf(field.getKey()) + 2;
        	 else
        		 index = index2.indexOf(field.getKey()) + 2;
        	 if (index !=1){
        		 //System.out.println("Node Type: " + field.getValue().getNodeType());
	        	 if(field.getValue().isInt() || field.getValue().isBoolean()){
	        		// System.out.println(index + " " + field.getKey() +  " NUMBER, " + field.getValue().asInt());
	        		 ps.setInt(index, field.getValue().asInt());
	        	 }else if( field.getValue().isDouble()){
	        		// System.out.println(index + " " + field.getKey() +  " DOUBLE, " + field.getValue().asDouble());
	        		 ps.setDouble(index, field.getValue().asDouble());
	        	 }else if( field.getValue().isLong()){
	        		 //System.out.println(index + " " + field.getKey() +  " Long, " + field.getValue().asLong());
	        		 ps.setLong(index, field.getValue().asLong());
	        	 }else{
	        		// System.out.println(index + " string " + field.getValue());
	        		 ps.setString(index, field.getValue().asText());
	        	 }
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

