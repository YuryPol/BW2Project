package com.bwing.invmanage2;

import java.io.IOException;
import java.nio.channels.Channels;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.appengine.api.utils.SystemProperty;
import com.google.appengine.tools.cloudstorage.GcsInputChannel;

public class InventoryState implements AutoCloseable
{
	String url;
	String customer_name;
    Connection con;
    Statement st;
    CallableStatement cs;
    PreparedStatement prepStatement;
    private static ObjectMapper mapper = new ObjectMapper();
    
    // Tables
    public static final String raw_inventory_ex = "_raw_inventory_ex";
    public static final String structured_data_inc ="_structured_data_inc";
    public static final String structured_data_base = "_structured_data_base";
    
    public InventoryState(String name) throws ClassNotFoundException, SQLException
	{
    	customer_name = name;
		// Connect to DB
		if (SystemProperty.environment.value() == SystemProperty.Environment.Value.Production) 
		{
			// Load the class that provides the new "jdbc:google:mysql://"
			// prefix.
			Class.forName("com.mysql.jdbc.GoogleDriver");
			url = "jdbc:google:mysql://<your-project-id>:<your-instance-name>/<your-database-name>?user=root";
		} 
		else 
		{
			// Local MySQL instance to use during development.
			Class.forName("com.mysql.jdbc.Driver"); // can't find the class
			url = "jdbc:mysql://localhost:3306/demo?user=root&password=IraAnna12";
		}
    	con = DriverManager.getConnection(url);		
	}
    
    public void Init() throws SQLException
    {
    	// Create tables 
    	prepStatement = con.prepareStatement(
    	"USE DEMO;"
    		+ "CREATE TABLE ? ("
		    + "set_key_is BIGINT DEFAULT NULL,"
		    + "set_key BIGINT DEFAULT NULL,"
		    + "set_name VARCHAR(20) DEFAULT NULL,"
		    + "capacity INT DEFAULT NULL, "
		    + "availability INT DEFAULT NULL," 
		    + "goal INT DEFAULT 0,"
		    + "criteria VARCHAR(200) DEFAULT NULL,"
		    + "PRIMARY KEY(set_key_is));");
    	prepStatement.setString(1, customer_name + structured_data_base);
    	prepStatement.execute();
    }
    
    public void Clean() throws SQLException
    {
    	// Truncate all tables
    	prepStatement = con.prepareStatement("USE DEMO; TRUNCATE ?");
    	prepStatement.setString(1, customer_name + structured_data_base);
    	prepStatement.execute();
    }
    
    public boolean isLoaded() throws SQLException
    {
		// Do the tables exist?
    	prepStatement = con.prepareStatement("USE DEMO; SELECT 1 FROM ? LIMIT 1");
    	prepStatement.setString(1, customer_name + structured_data_base);
    	java.sql.ResultSet rs = prepStatement.executeQuery();
//    	java.sql.ResultSet rs = con.getMetaData().getTables(null, null, customer_name + "_raw_inventory_ex", null);
        if (!rs.next())
        	return false;
        else
        	return true;
    }
    
    public void Load(GcsInputChannel readChannel) throws JsonParseException, JsonMappingException, IOException
    {
		//convert json input to InventroryData object
		InventroryData inventorydata= mapper.readValue(Channels.newInputStream(readChannel), InventroryData.class);
		// Populate all tables 	
    }
    
    public void close() throws SQLException
    {
    	con.close();
    }

}
