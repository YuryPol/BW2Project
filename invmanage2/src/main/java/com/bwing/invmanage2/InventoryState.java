package com.bwing.invmanage2;

import java.io.IOException;
import java.nio.channels.Channels;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.BitSet;
import java.util.HashMap;
import java.util.logging.Logger;

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
    private static ObjectMapper mapper = new ObjectMapper();
    private static final Logger log = Logger.getLogger(InventoryState.class.getName());
    
    // Tables
    static final String raw_inventory_ex = "_raw_inventory_ex";
    static final String raw_inventory = "_raw_inventory";
    static final String structured_data_inc ="_structured_data_inc";
    static final String structured_data_base = "_structured_data_base";
    static final String unions_last_rank = "_unions_last_rank";
    static final String unions_next_rank = "_unions_next_rank";
 
	static final int BITMAP_SIZE = 64;
    
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
			url = "jdbc:mysql://localhost:3306/BWdemo?user=root&password=IraAnna12";
		}
    	con = DriverManager.getConnection(url);		
    	try (Statement st = con.createStatement())
    	{
    		st.execute("USE BWdemo");
    	}
	}
    
    public void init() throws SQLException
    {
    	// Create tables 
        try (Statement st = con.createStatement())
        {
        	// create raw_inventory_ex table to fill up by impressons' counts
        	st.executeUpdate("DROP TABLE IF EXISTS " + customer_name + raw_inventory_ex);
        	st.executeUpdate("CREATE TABLE " + customer_name + raw_inventory_ex 
        	+ " (basesets BIGINT NOT NULL, "
            + "count INT NOT NULL, "
            + "criteria VARCHAR(200) DEFAULT NULL)");

        	// create raw_inventory table to fill it from raw_inventory_ex with compacted and weighted data
        	st.executeUpdate("DROP TABLE IF EXISTS " + customer_name + raw_inventory);
        	st.executeUpdate("CREATE TABLE " + customer_name + raw_inventory 
        	+ " (basesets BIGINT NOT NULL, "
            + "count INT NOT NULL, "
            + "weight BIGINT DEFAULT 0)");

        	// create structured data table
        	st.executeUpdate("DROP TABLE IF EXISTS " + customer_name + structured_data_inc);
        	st.executeUpdate("CREATE TABLE " + customer_name +  structured_data_inc
    	    + " (set_key BIGINT DEFAULT NULL, "
    	    + "set_name VARCHAR(20) DEFAULT NULL, "
    	    + "capacity INT DEFAULT NULL, " 
    	    + "availability INT DEFAULT NULL, " 
    	    + "goal INT DEFAULT 0, "
    	    + "PRIMARY KEY(set_key))");        	
        	
        	// create inventory sets table
        	st.executeUpdate("DROP TABLE IF EXISTS " + customer_name + structured_data_base);
        	st.executeUpdate("CREATE TABLE " + customer_name + structured_data_base 
        	+ " (set_key_is BIGINT DEFAULT NULL, "
		    + "set_key BIGINT DEFAULT NULL, "
		    + "set_name VARCHAR(20) DEFAULT NULL, "
		    + "capacity INT DEFAULT NULL, "
		    + "availability INT DEFAULT NULL, " 
		    + "goal INT DEFAULT 0, "
		    + "criteria VARCHAR(200) DEFAULT NULL, "
		    + "PRIMARY KEY(set_key_is))");

        	// create temporary table to insert next rank rows
        	st.executeUpdate("DROP TABLE IF EXISTS " + customer_name + unions_last_rank);
        	st.executeUpdate("CREATE TABLE " + customer_name +  unions_last_rank
    	    + " (set_key BIGINT DEFAULT NULL, "
    	    + "set_name VARCHAR(20) DEFAULT NULL, "
    	    + "capacity INT DEFAULT NULL, " 
    	    + "availability INT DEFAULT NULL, " 
    	    + "goal INT DEFAULT 0, "
    	    + "PRIMARY KEY(set_key))");
        	
        	st.executeUpdate("DROP TABLE IF EXISTS " + customer_name + unions_next_rank);
        	st.executeUpdate("CREATE TABLE " + customer_name + unions_next_rank
    	    + " (set_key BIGINT DEFAULT NULL, "
    	    + "set_name VARCHAR(20) DEFAULT NULL, "
    	    + "capacity INT DEFAULT NULL, " 
    	    + "availability INT DEFAULT NULL, " 
    	    + "goal INT DEFAULT 0, "
    	    + "PRIMARY KEY(set_key))");
        }
    }
    
    public void clear() throws SQLException
    {
    	// Truncate all tables
        try (Statement st = con.createStatement())
        {
        	st.executeUpdate("TRUNCATE " + customer_name + raw_inventory_ex);
        	st.executeUpdate("TRUNCATE " + customer_name + raw_inventory);
        	st.executeUpdate("TRUNCATE " + customer_name + structured_data_inc);
        	st.executeUpdate("TRUNCATE " + customer_name + structured_data_base);
        	st.executeUpdate("TRUNCATE " + customer_name + unions_last_rank);
        	st.executeUpdate("TRUNCATE " + customer_name + unions_next_rank);
         }
    }
    
    public boolean isLoaded() throws SQLException
    {
		// Do the tables exist?
        try (Statement st = con.createStatement())
        {
        	java.sql.ResultSet rs = st.executeQuery("SELECT count(*) FROM " + customer_name + structured_data_base);
    //    	java.sql.ResultSet rs = st.executeQuery("SELECT 1 FROM " + customer_name + structured_data_base + " LIMIT 1");
	//    	java.sql.ResultSet rs = con.getMetaData().getTables(null, null, customer_name + "_raw_inventory_ex", null);
	        if (!rs.next() || rs.getLong(1) == 0)
	        	return false;
	        else
	        	return true;
        }
    }
    
    public void load(GcsInputChannel readChannel) throws JsonParseException, JsonMappingException, IOException, SQLException
    {
		//convert json input to InventroryData object
		InventroryData inventorydata= mapper.readValue(Channels.newInputStream(readChannel), InventroryData.class);
		// Create inventory sets data. TODO: write into DB from the start
		HashMap<BitSet, BaseSet> base_sets = new HashMap<BitSet, BaseSet>();			
		int highBit = 0;
		for (inventoryset is : inventorydata.getInventorysets())
		{
			BaseSet tmp = new BaseSet(BITMAP_SIZE);
			tmp.setkey(highBit);
			tmp.setname(is.getName());
			tmp.setCriteria(is.getcriteria());
			base_sets.put(tmp.getkey(), tmp);
			highBit++;
		}
		if (highBit == 0)
		{
			log.warning("no data in inventory sets in " + readChannel.toString());
			return;
		}			
		
		// Create segments' raw data. TODO: write into DB from the start
		HashMap<BitSet, BaseSegement> base_segments = new HashMap<BitSet, BaseSegement>();
		for (segment seg : inventorydata.getSegments())
		{
			boolean match_found = false;
			BaseSegement tmp = new BaseSegement(BITMAP_SIZE);
			tmp.setCriteria(seg.getcriteria());
			
			for (BaseSet bs1 : base_sets.values())
			{					
				if (bs1.getCriteria().matches(tmp.getCriteria()))
				{
					tmp.getkey().or(bs1.getkey());
					match_found = true;
				}
			}
			if (match_found) 
			{
				tmp.setcapacity(seg.getCount());
				base_segments.put(tmp.getkey(), tmp);
			}
		}
		if (base_segments.isEmpty())
		{
			log.warning("no data in segments " + readChannel.toString());
			return;
		}

		//
		// Populate all tables 
		//
        // populate structured data with inventory sets
        try (PreparedStatement insertStatement = con.prepareStatement("INSERT IGNORE INTO "  + customer_name + structured_data_base 
        		+ " SET set_key = ?, set_name = ?, set_key_is = ?, criteria = ?"))
        {
	        for (BaseSet bs1 : base_sets.values()) {
	         	insertStatement.setLong(1, bs1.getKeyBin()[0]);
	            insertStatement.setString(2, bs1.getname());
	        	insertStatement.setLong(3, bs1.getKeyBin()[0]);
	        	insertStatement.setString(4, bs1.getCriteria().toString());
	            insertStatement.execute();
	        }
        }
        
        // populate raw data with inventory sets' bitmaps
        try (PreparedStatement insertStatement = con.prepareStatement("INSERT INTO "  + customer_name + raw_inventory_ex 
        		+ " SET basesets = ?, count = ?, criteria = ?"))
        {
	        for (BaseSegement bs1 : base_segments.values()) {
	        	insertStatement.setLong(1, bs1.getKeyBin()[0]);
	        	insertStatement.setInt(2, bs1.getcapacity());
	        	insertStatement.setString(3, bs1.getCriteria().toString());
	            insertStatement.execute();
	        }
        }
        
        // adds up multiple records in raw_inventory_ex with the same key
        try (PreparedStatement insertStatement = con.prepareStatement("INSERT INTO " + customer_name + raw_inventory
        		+ " SELECT basesets, sum(count) as count, 0 as weight FROM " + customer_name + raw_inventory_ex 
        		+ " GROUP BY basesets"))
        {
        	insertStatement.execute();
        } // That shouldn't be necessary as raw_inventory_ex already groups them but verification is needed.
        
        // update raw inventory with weights
        try (PreparedStatement insertStatement = con.prepareStatement("SELECT @n:=0"))
        {
        	insertStatement.execute();
        }
        try (PreparedStatement insertStatement = con.prepareStatement("UPDATE " + customer_name + raw_inventory
        		+ " SET weight = @n := @n + " + customer_name + raw_inventory + ".count"))
        {
        	insertStatement.execute();
        }

        // adds capacities and availabilities to structured data
        try (PreparedStatement insertStatement = con.prepareStatement("UPDATE " + customer_name + structured_data_base + " sdb, "
        		+ " (SELECT set_key, SUM(ri.count) AS capacity, SUM(ri.count) AS availability "
        		+ " FROM " + customer_name + structured_data_base
        		+ " JOIN " + customer_name + raw_inventory + " ri ON set_key & ri.basesets != 0 "
        		+ " GROUP BY set_key) comp SET sdb.capacity = comp.capacity, "
        		+ " sdb.availability = comp.availability WHERE sdb.set_key = comp.set_key"))
        {
        	insertStatement.execute();        	
        }
        
        // populate inventory sets table
        try (PreparedStatement insertStatement = con.prepareStatement("INSERT INTO " + customer_name + structured_data_inc
        		+ " SELECT set_key, set_name, capacity, availability, goal FROM " + customer_name + structured_data_base
        		+ " WHERE capacity IS NOT NULL"))
        {       	
        	insertStatement.execute();        	
        }
        
        // start union_next_rank table
        try (PreparedStatement insertStatement = con.prepareStatement("INSERT INTO " + customer_name + unions_next_rank
        		+ " SELECT * FROM " + customer_name + structured_data_inc))
        {
        	insertStatement.execute();        	
        }
        
        // adds unions of higher ranks for all nodes to structured_data_inc
        try (CallableStatement callStatement = con.prepareCall("{call AddUnions(?, ?, ?, ?, ?)}"))
        {
           	callStatement.setString(1, customer_name + structured_data_inc);
           	callStatement.setString(2, customer_name + unions_last_rank);
           	callStatement.setString(3, customer_name + unions_next_rank);
           	callStatement.setString(4, customer_name + raw_inventory);
           	callStatement.setString(5, customer_name + structured_data_base);
           	callStatement.execute();
        }

    }
    
    @Override
    public void close() throws SQLException
    {
    	if(con!=null)
    		con.close();
    }

}
