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
import java.util.Properties;
import java.util.logging.Logger;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.appengine.api.utils.SystemProperty;
import com.google.appengine.tools.cloudstorage.GcsInputChannel;
import java.sql.ResultSet;
;

public class InventoryState implements AutoCloseable
{
	String url;
	String customer_name;
    Connection con;
    private static ObjectMapper mapper = new ObjectMapper();
    private static final Logger Log = Logger.getLogger(InventoryState.class.getName());
    
    // DB prefix
    public static final String BWdb = "BWing_";
    
    // Tables
    static final String raw_inventory_ex = "raw_inventory_ex";
    static final String raw_inventory = "raw_inventory";
    static final String structured_data_inc ="structured_data_inc";
    static final String structured_data_base = "structured_data_base";
    static final String unions_last_rank = "unions_last_rank";
    static final String unions_next_rank = "unions_next_rank";
 
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
			url = "jdbc:google:mysql://bw2project:bw2project?user=root&password=IraAnna12";
		} 
		else 
		{
			// Local MySQL instance to use during development.
			Class.forName("com.mysql.jdbc.Driver"); // can't find the class
			url = "jdbc:mysql://localhost:3306?user=root&password=IraAnna12";
		}
		Properties info = new Properties();
		info.setProperty("connectTimeout", "0");
		info.setProperty("socketTimeout", "0");
    	con = DriverManager.getConnection(url, info);		
    	con.setCatalog(BWdb + customer_name);
    	// st.execute("USE " + BWdb + customer_name);
	}
    
    public Connection getConnection() throws SQLException
    {
    	return con;
    }
    
    public void init() throws SQLException
    {
    	// Create tables, stored procedures and functions 
        try (Statement st = con.createStatement())
        {
        	// Create the database and start using it
        	st.executeUpdate("DROP DATABASE IF EXISTS " + BWdb + customer_name);
        	st.executeUpdate("CREATE DATABASE " + BWdb + customer_name);        	
//        	st.execute("USE " + BWdb + customer_name);

       	// create raw_inventory_ex table to fill up by impressions' counts
        	st.executeUpdate("DROP TABLE IF EXISTS " + raw_inventory_ex);
        	st.executeUpdate("CREATE TABLE " + raw_inventory_ex 
        	+ " (basesets BIGINT NOT NULL, "
            + "count INT NOT NULL, "
            + "criteria VARCHAR(200) DEFAULT NULL)");

        	// create raw_inventory table to fill it from raw_inventory_ex with compacted and weighted data
        	st.executeUpdate("DROP TABLE IF EXISTS " + raw_inventory);
        	st.executeUpdate("CREATE TABLE " + raw_inventory 
        	+ " (basesets BIGINT NOT NULL, "
            + "count INT NOT NULL, "
            + "weight BIGINT DEFAULT 0)");

        	// create structured data table
        	st.executeUpdate("DROP TABLE IF EXISTS " + structured_data_inc);
        	st.executeUpdate("CREATE TABLE " +  structured_data_inc
    	    + " (set_key BIGINT DEFAULT NULL, "
    	    + "set_name VARCHAR(20) DEFAULT NULL, "
    	    + "capacity INT DEFAULT NULL, " 
    	    + "availability INT DEFAULT NULL, " 
    	    + "goal INT DEFAULT 0, "
    	    + "PRIMARY KEY(set_key))");        	
        	
        	// create inventory sets table
        	st.executeUpdate("DROP TABLE IF EXISTS " + structured_data_base);
        	st.executeUpdate("CREATE TABLE " + structured_data_base 
        	+ " (set_key_is BIGINT DEFAULT NULL, "
		    + "set_key BIGINT DEFAULT NULL, "
		    + "set_name VARCHAR(20) DEFAULT NULL, "
		    + "capacity INT DEFAULT NULL, "
		    + "availability INT DEFAULT NULL, " 
		    + "goal INT DEFAULT 0, "
		    + "criteria VARCHAR(200) DEFAULT NULL, "
		    + "PRIMARY KEY(set_key_is))");

        	// create temporary table to insert next rank rows
        	st.executeUpdate("DROP TABLE IF EXISTS " + unions_last_rank);
        	st.executeUpdate("CREATE TABLE " +  unions_last_rank
    	    + " (set_key BIGINT DEFAULT NULL, "
    	    + "set_name VARCHAR(20) DEFAULT NULL, "
    	    + "capacity INT DEFAULT NULL, " 
    	    + "availability INT DEFAULT NULL, " 
    	    + "goal INT DEFAULT 0, "
    	    + "PRIMARY KEY(set_key))");
        	
        	st.executeUpdate("DROP TABLE IF EXISTS " + unions_next_rank);
        	st.executeUpdate("CREATE TABLE " + unions_next_rank
    	    + " (set_key BIGINT DEFAULT NULL, "
    	    + "set_name VARCHAR(20) DEFAULT NULL, "
    	    + "capacity INT DEFAULT NULL, " 
    	    + "availability INT DEFAULT NULL, " 
    	    + "goal INT DEFAULT 0, "
    	    + "PRIMARY KEY(set_key))");
        	
        	st.executeUpdate("DROP PROCEDURE IF EXISTS PopulateRankWithNumbers");
        	st.executeUpdate("CREATE PROCEDURE PopulateRankWithNumbers() "
        			+ "BEGIN "
        			+ " UPDATE unions_next_rank nr0, "
        			+ " (SELECT "
        			+ "    set_key, "
        			+ "    SUM(capacity) as capacity, "
        			+ "    SUM(availability) as availability "
        			+ "  FROM ("
        			+ "   SELECT nr.set_key, ri.count as capacity, ri.count as availability "
        			+ "    FROM unions_next_rank nr "
        			+ "    JOIN raw_inventory ri "
        			+ "    ON nr.set_key & ri.basesets != 0 "
        			+ "    WHERE nr.capacity is NULL "
        			+ "    ) blownUp "
        			+ "  GROUP BY set_key"
        			+ " ) comp "
        			+ " SET nr0.capacity = comp.capacity,"
        			+ "     nr0.availability = comp.availability "
        			+ " WHERE nr0.set_key = comp.set_key; "
        			+ " END "
        			);
        	
        	st.executeUpdate("DROP PROCEDURE IF EXISTS AddUnions");
        	st.executeUpdate("CREATE PROCEDURE AddUnions() "
        			+ "BEGIN "
        			+ "    DECLARE cnt INT;"
        			+ "    DECLARE cnt_updated INT;"
        			
        			+ "    REPEAT "
        			+ "    SELECT count(*) INTO cnt FROM structured_data_inc;"
        			
        			+ "    TRUNCATE unions_last_rank;"
        			
        			+ "    INSERT INTO unions_last_rank "
        			+ "	   SELECT * FROM unions_next_rank;"
        			
        			+ "	TRUNCATE unions_next_rank;"
        			
        			+ "	INSERT /*IGNORE*/ INTO unions_next_rank "
        			+ "    SELECT sb.set_key_is | lr.set_key, NULL, NULL, NULL, 0 "
        			+ "	   FROM unions_last_rank lr "
        			+ "    JOIN structured_data_base sb "
        			+ "	   JOIN raw_inventory ri "
        			+ "    ON  (sb.set_key_is & ri.basesets != 0) "
        			+ "        AND (lr.set_key & ri.basesets) != 0 "
        			+ "        AND (sb.set_key_is | lr.set_key) != lr.set_key "
        			+ "    GROUP BY sb.set_key_is | lr.set_key; "
        			+ "    CALL PopulateRankWithNumbers; "
        			+ "    DELETE FROM structured_data_inc "
        			+ "    WHERE EXISTS ("
        			+ "        SELECT * "
        			+ "        FROM unions_next_rank nr "
        			+ "        WHERE (structured_data_inc.set_key & nr.set_key) = structured_data_inc.set_key "
        			+ "        AND structured_data_inc.capacity = nr.capacity); "
        			+ "    INSERT /*IGNORE*/ INTO structured_data_inc "
        			+ "    SELECT * FROM unions_next_rank; "
        			+ "    SELECT count(*) INTO cnt_updated FROM structured_data_inc; " 
        			+ "    UNTIL  (cnt = cnt_updated) "
        			+ "    END REPEAT; "
        			+ "    DELETE FROM structured_data_inc "
        			+ "    WHERE capacity IS NULL; "
        			+ "    UPDATE structured_data_base, structured_data_inc "
        			+ "    SET structured_data_base.set_key = structured_data_inc.set_key "
        			+ "    WHERE structured_data_base.set_key_is & structured_data_inc.set_key = structured_data_base.set_key_is "
        			+ "    AND structured_data_base.capacity = structured_data_inc.capacity; "
        			+ "END "
        			);
        	
        	st.executeUpdate("DROP FUNCTION IF EXISTS BookItemsFromIS");
        	st.executeUpdate("CREATE FUNCTION BookItemsFromIS(iset BIGINT, amount INT) "
        			+ "RETURNS BOOLEAN "
        			+ "DETERMINISTIC "
        			+ "READS SQL DATA "
        			+ "BEGIN "
        			+ "    DECLARE cnt INT; "
        			+ "    SELECT availability INTO cnt FROM structured_data_base WHERE set_key_is = iset; "
        			+ "    IF cnt >= amount AND amount > 0 "
        			+ "    THEN "
        			+ "     UPDATE structured_data_base "
        			+ "     SET availability=availability-amount, goal=goal+amount "
        			+ "     WHERE set_key_is = iset; "
        			+ "     RETURN TRUE; "
        			+ "    ELSE "
        			+ "     RETURN FALSE; "
        			+ "    END IF; "
        			+ "END "
        			);
        	
        	st.executeUpdate("DROP PROCEDURE IF EXISTS GetItemsFromSD");
        	st.executeUpdate("CREATE PROCEDURE GetItemsFromSD(IN iset BIGINT, IN amount INT) "
        			+ "BEGIN "
        			+ "IF BookItemsFromIS(iset, amount) "
        			+ "   THEN "
        			+ "     UPDATE structured_data_inc "
        			+ "        SET availability = availability - amount "
        			+ "        WHERE (set_key & iset) = iset; "
        			+ "       DELETE FROM structured_data_inc WHERE set_key = ANY ( "
        			+ "       SELECT set_key FROM ( "
        			+ "          SELECT sd1.set_key "
        			+ "          FROM structured_data_inc sd1 JOIN structured_data_inc sd2 "
        			+ "          ON sd2.set_key > sd1.set_key "
        			+ "          AND sd2.set_key & sd1.set_key = sd1.set_key "
        			+ "          AND sd1.availability >= sd2.availability) AS stmp "
        			+ "       ); "
        			+ "     UPDATE structured_data_base sb, structured_data_inc sd "
        			+ "     SET sb.availability = LEAST(sb.availability, sd.availability) "
        			+ "     WHERE sd.set_key & sb.set_key_is = sb.set_key_is; "
        			+ "     SELECT 'passed'; "
        			+ "   ELSE "
        			+ "     SELECT 'failed'; "
        			+ "   END IF; "
        			+ "END "
        			);
        }
    }
    
    public void clear() throws SQLException
    {
    	// Truncate all tables
        try (Statement st = con.createStatement())
        {
//        	st.execute("USE " + BWdb + customer_name);      	
        	st.executeUpdate("TRUNCATE " + raw_inventory_ex);
        	st.executeUpdate("TRUNCATE " + raw_inventory);
        	st.executeUpdate("TRUNCATE " + structured_data_inc);
        	st.executeUpdate("TRUNCATE " + structured_data_base);
        	st.executeUpdate("TRUNCATE " + unions_last_rank);
        	st.executeUpdate("TRUNCATE " + unions_next_rank);
         }
    }
    
    public boolean isLoaded() throws SQLException
    {
		// Do the tables exist?
        try (Statement st = con.createStatement())
        {
//        	st.execute("USE " + BWdb + customer_name);      	
        	java.sql.ResultSet rs = st.executeQuery("SELECT count(*) FROM " + structured_data_base);
    //    	java.sql.ResultSet rs = st.executeQuery("SELECT 1 FROM " + structured_data_base + " LIMIT 1");
	//    	java.sql.ResultSet rs = con.getMetaData().getTables(null, null, customer_name + "_raw_inventory_ex", null);
	        if (!rs.next() || rs.getLong(1) == 0)
	        	return false;
	        else
	        	return true;
        }
    }
    
    public void lock() throws SQLException
    {
        try (Statement st = con.createStatement())
        {
        	st.executeQuery("LOCK TABLE "+ raw_inventory_ex + " WRITE, "
        			+ raw_inventory + " WRITE, "
        			+ structured_data_inc + " WRITE, "
        			+ structured_data_base + " WRITE, "
        			+ unions_last_rank + " WRITE, "
        			+ unions_next_rank + " WRITE, "
        			+ structured_data_base + " AS sdbW WRITE, " 
        			+ structured_data_base + " AS sdbR READ, "
        			+ raw_inventory + " AS ri READ"
        			);
        }
    }
       
    public void unlock() throws SQLException
    {
        try (Statement st = con.createStatement())
        {
        	st.executeQuery("UNLOCK TABLES");
        	Log.warning("databases unlocked");
       }
    }
    
    public void invalidate() throws SQLException
    {
        try (Statement st = con.createStatement())
        {
        	clear();
        	st.executeUpdate("INSERT IGNORE INTO " + structured_data_base + " SET goal = -2147483648");
        	Log.warning("status invalidated");
        }
    }
       
    public boolean isValid() throws SQLException
    {
        try (Statement st = con.createStatement())
        {
        	java.sql.ResultSet rs = st.executeQuery("SELECT count(*) FROM " + structured_data_base 
        		+ " WHERE goal = -2147483648");
	        if (!rs.next() || rs.getLong(1) == 0)
	        {
	        	return false;
	        }
	        else
	        {
	        	return true;
	        }
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
			Log.warning("no data in inventory sets in " + readChannel.toString());
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
			Log.warning("no data in segments " + readChannel.toString());
			return;
		}

		//
		// Populate all tables 
		//
        // populate structured data with inventory sets
//        try (Statement st = con.createStatement())
//        {
//        	st.execute("USE " + BWdb + customer_name);
//        }
        
        try (PreparedStatement insertStatement = con.prepareStatement("INSERT IGNORE INTO "  + structured_data_base 
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
        try (PreparedStatement insertStatement = con.prepareStatement("INSERT INTO "  + raw_inventory_ex 
        		+ " SET basesets = ?, count = ?, criteria = ?"))
        {
        	Log.info("INSERT INTO "  + raw_inventory_ex);
	        for (BaseSegement bs1 : base_segments.values()) {
	        	insertStatement.setLong(1, bs1.getKeyBin()[0]);
	        	insertStatement.setInt(2, bs1.getcapacity());
	        	insertStatement.setString(3, bs1.getCriteria().toString());
	            insertStatement.execute();
	        }
        }
        
        // adds up multiple records in raw_inventory_ex with the same key
        try (Statement st = con.createStatement())
        {
        	st.executeUpdate("INSERT INTO " + raw_inventory
        		+ " SELECT basesets, sum(count) as count, 0 as weight FROM " + raw_inventory_ex 
        		+ " GROUP BY basesets");
        	Log.info("INSERT INTO " + raw_inventory);
        } // That shouldn't be necessary as raw_inventory_ex already groups them but verification is needed.
        
        // update raw inventory with weights
        try (PreparedStatement st = con.prepareStatement("SELECT @n:=0"))
        {
        	st.execute();
        }
        
        try (PreparedStatement st = con.prepareStatement("UPDATE " + raw_inventory
        		+ " SET weight = @n := @n + " + raw_inventory + ".count"))
        {
        	Log.info("UPDATE " + raw_inventory);
        	st.execute();
        }

        // adds capacities and availabilities to structured data
        try (Statement st = con.createStatement())
        {
        	
        	st.executeUpdate("UPDATE "
        			+ structured_data_base + " AS sdbW, "
        			+ " (SELECT set_key, SUM(ri.count) AS capacity, SUM(ri.count) AS availability FROM "
        			+ structured_data_base + " AS sdbR "
        			+ " JOIN " 
        			+ raw_inventory + " AS ri "
        			+ " ON set_key & ri.basesets != 0 "
        			+ " GROUP BY set_key) comp "
        			+ " SET sdbW.capacity = comp.capacity, "
        			+ " sdbW.availability = comp.availability "
        			+ " WHERE sdbW.set_key = comp.set_key");
        	Log.info("UPDATE " + structured_data_base);
        	
        	// populate inventory sets table
        	st.executeUpdate("INSERT INTO " 
        			+ structured_data_inc
        			+ " SELECT set_key, set_name, capacity, availability, goal FROM " 
        			+ structured_data_base 
        			+ " WHERE capacity IS NOT NULL");
        	Log.info("INSERT INTO " + structured_data_inc);
        }
        
        // start union_next_rank table
        try (PreparedStatement insertStatement = con.prepareStatement("INSERT INTO " + unions_next_rank
        		+ " SELECT * FROM " + structured_data_inc))
        {
        	Log.info("INSERT INTO " + unions_next_rank);
        	insertStatement.execute();        	
        }
        
        // adds unions of higher ranks for all nodes to structured_data_inc
        try (CallableStatement callStatement = con.prepareCall("{call " + BWdb + customer_name + ".AddUnions()}"))
        {
        	Log.info("{call " + BWdb + customer_name + ".AddUnions()}");
           	callStatement.execute();
        }
        Log.info("Inventory for " + customer_name + " was loaded!");
    }
    
    public void GetItems(String set_name, int amount) throws SQLException
    {
    	if (amount <= 0)
    	{
			Log.warning("wrong allocation = " + amount);
			return;
    	}
    	long set_key_is = 0;
    	
    	String query = "SELECT set_key_is FROM structured_data_base WHERE set_name = '" + set_name + "'";
    	try (Statement statement = con.createStatement())
    	{
    		ResultSet rs = statement.executeQuery(query);
    		rs.next();
    		set_key_is = rs.getLong(1);
    	}
    	try (CallableStatement callStatement = con.prepareCall("{call " + BWdb + customer_name + ".GetItemsFromSD(?, ?)}"))
    	{
    		callStatement.setLong(1, set_key_is);
    		callStatement.setInt(2, amount);
    		callStatement.execute();
    	}    			
    }
    
    
    @Override
    public void close() throws SQLException
    {
    	if(con!=null)
    	{
    		unlock();
    		con.close();
    	}
    }

}
