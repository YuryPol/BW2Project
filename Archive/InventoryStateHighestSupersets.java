package com.bwing.invmanage2;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.appengine.api.utils.SystemProperty;
import com.mysql.jdbc.exceptions.jdbc4.CommunicationsException;

import java.sql.ResultSet;
;

public class InventoryState implements AutoCloseable
{
	public enum Status {
		clean, loadstarted, loadinprogress, wrongfile, inconsitent, toomuchdata, loaded
		, highoverlap, manysegmens, nosegments, nodata, unknown
	}

	String customer_name = "";
    Connection con;
    private static ObjectMapper mapper = new ObjectMapper();
    private static final Logger log = Logger.getLogger(InventoryState.class.getName());
    
    // DB prefix
    public static final String BWdb = "BWing_";
    
    // Tables
    public static final String raw_inventory = "raw_inventory";
    static final String structured_data_inc ="structured_data_inc";
    static final String structured_data_base = "structured_data_base";
    static final String unions_last_rank = "unions_last_rank";
    static final String unions_next_rank = "unions_next_rank";
    public static final String allocation_ledger = "allocation_protocol";
    public static final String result_serving = "result_serving";
    public static final String result_serving_copy = "result_serving_copy";
    private static final String ex_inc_unions = "ex_inc_unions";
    private static final String ex_inc_unions1 = "ex_inc_unions1";
    private static final String temp_unions = "temp_unions";
    static final String inventory_status = "inventory_status";
 
    static public boolean DEBUG = false;
	static public int BITMAP_MAX_SIZE = 60; // max = 64;
	static public int CARDINALITY_LIMIT = 10;
	static public int INVENTORY_OVERLAP = 500;
	static public int BASE_SETS_OVERLAPS_LIMIT = 100;
	private static long RESTART_INTERVAL = 600000 - 10000; // less than 10 minutes
	private static int MAX_ROW_SIZE = 3000;
	private static double OVERLAP_FRACTION = 0.1;	
	private TimeoutHandler timeoutHandler = new TimeoutHandler();
	
	public class TimeoutHandler {
		static final int sleepMin = 1;
		
		public void reconnect() throws SQLException, InterruptedException, ClassNotFoundException
		{
			// reconnect to db
	    	log.severe(customer_name + " : Hopefully MySQL completes it in " + String.valueOf(sleepMin) + " min.");
	    	TimeUnit.MINUTES.sleep(1);
	    	if (con != null)
	    	{
	    		con.close();
	    	}
	    	con = connect(true);
	    	con.setCatalog(InventoryState.BWdb + customer_name);
		}
	}

	public static Connection connect(boolean auto) throws ClassNotFoundException, SQLException
	{
		String url;
		if (SystemProperty.environment.value() == SystemProperty.Environment.Value.Production) 
		{
			// Load the class that provides the new "jdbc:google:mysql://"
			// prefix.
			Class.forName("com.mysql.jdbc.GoogleDriver");
			url = "jdbc:google:mysql://bw2project:bw2project?user=root&password=IraAnna12"; // This is for First generation SQL
//			url = "jdbc:google:mysql://bw2project:us-central1:bwing2gen?user=root&password=IraAnna12";	// This is for Second generation SQL
		} 
		else 
		{
			// Local MySQL instance to use during development.
			Class.forName("com.mysql.jdbc.Driver"); // can't find the class
			url = "jdbc:mysql://localhost:3306?user=root&password=IraAnna12&autoReconnect=true&useSSL=false";
		}

//	    if (System
//	            .getProperty("com.google.appengine.runtime.version").startsWith("Google App Engine/")) {
//	          // Check the System properties to determine if we are running on appengine or not
//	          // Google App Engine sets a few system properties that will reliably be present on a remote
//	          // instance.
//	          url = System.getProperty("ae-cloudsql.cloudsql-database-url");
//	          try {
//	            // Load the class that provides the new "jdbc:google:mysql://" prefix.
//	            Class.forName("com.mysql.jdbc.GoogleDriver");
//	          } catch (ClassNotFoundException e) {
//	        	  log.severe(e.fillInStackTrace().toString());
//	        	  e.printStackTrace();
//	          }
//	        } else {
//	          // Set the url with the local MySQL database connection url when running locally
//	          url = System.getProperty("ae-cloudsql.local-database-url");
//	        }
		
		Properties info = new Properties();
		info.setProperty("connectTimeout", "0");
		info.setProperty("socketTimeout", "0");
		Connection con = DriverManager.getConnection(url, info);
		con.setAutoCommit(auto);
		return con;
	}
    
    public InventoryState(String name, boolean auto) throws ClassNotFoundException, SQLException, IOException
	{
    	customer_name = name;
		log.setLevel(Level.INFO);
    	con = connect(auto);
		// Connect to DB
    	con.setCatalog(BWdb + customer_name);
        try (Statement st = con.createStatement())
        {
        	st.execute("USE " + BWdb + customer_name);
        }
        InputStream inventory_properties_stream = this.getClass().getClassLoader().getResourceAsStream("inventory.properties");
        if (inventory_properties_stream == null)
        {
        	inventory_properties_stream = new FileInputStream("./src/main/webapp/WEB-INF/classes/inventory.properties");
        }
        Properties inventory_properties = new Properties();
        inventory_properties.load(inventory_properties_stream);
        DEBUG = Boolean.valueOf(inventory_properties.getProperty("DEBUG"));
    	BITMAP_MAX_SIZE = Integer.valueOf(inventory_properties.getProperty("BITMAP_MAX_SIZE"));
    	CARDINALITY_LIMIT = Integer.valueOf(inventory_properties.getProperty("CARDINALITY_LIMIT"));
    	INVENTORY_OVERLAP = Integer.valueOf(inventory_properties.getProperty("INVENTORY_OVERLAP"));
    	BASE_SETS_OVERLAPS_LIMIT = Integer.valueOf(inventory_properties.getProperty("BASE_SETS_OVERLAPS_LIMIT"));
    	RESTART_INTERVAL = Long.valueOf(inventory_properties.getProperty("RESTART_INTERVAL")); // less than 10 minutes
    	MAX_ROW_SIZE = Integer.valueOf(inventory_properties.getProperty("MAX_ROW_SIZE"));
    	OVERLAP_FRACTION = Double.valueOf(inventory_properties.getProperty("OVERLAP_FRACTION"));
	}
    
    public Connection getConnection() throws SQLException
    {
    	if (con.isValid(5))
    		return con;
    	else
    		throw new SQLException("Connection is stale");
    }
    
    public static void init(String customer_name) throws SQLException, ClassNotFoundException
    {
    	// Create tables, stored procedures and functions
    	Connection conect = connect(true);
        try (Statement st = conect.createStatement())
        {
        	// Create the database and start using it
        	st.executeUpdate("DROP DATABASE IF EXISTS " + BWdb + customer_name);
        	st.executeUpdate("CREATE DATABASE " + BWdb + customer_name);
        	st.execute("USE " + BWdb + customer_name);
        	
        	// create status table 
        	st.executeUpdate("DROP TABLE IF EXISTS " + inventory_status);
        	st.executeUpdate("CREATE TABLE " + inventory_status 
                	+ " (fake_key INT DEFAULT 1, "
                	+ " status VARCHAR(200) DEFAULT '" + Status.clean.name()
                	+ "', PRIMARY KEY(fake_key))"
         	);
        }
    }
    
        	
    public void clear() throws SQLException
    {
    	// Recreate all tables
        try (Statement st = con.createStatement())
        {

        	// create raw_inventory table to fill up by impressions' counts

        	// create structured data table
        	st.executeUpdate("DROP TABLE IF EXISTS " + structured_data_inc);
        	st.executeUpdate("CREATE TABLE " +  structured_data_inc
    	    + " (set_key BIGINT DEFAULT 0, "
    	    + "set_name VARCHAR(20) DEFAULT NULL, "
    	    + "capacity INT DEFAULT NULL, " 
    	    + "availability INT DEFAULT NULL, " 
    	    + "goal INT DEFAULT 0, "
    	    + "PRIMARY KEY(set_key))");        	
        	
        	// create inventory sets table
        	st.executeUpdate("DROP TABLE IF EXISTS " + structured_data_base);
        	st.executeUpdate("CREATE TABLE " + structured_data_base 
        	+ " (set_key_is BIGINT DEFAULT 0, "
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
    	    + " (set_key BIGINT DEFAULT 0, "
    	    + "set_name VARCHAR(20) DEFAULT NULL, "
    	    + "capacity INT DEFAULT NULL, " 
    	    + "availability INT DEFAULT NULL, " 
    	    + "goal INT DEFAULT 0, "
    	    + "PRIMARY KEY(set_key)) ENGINE=MEMORY");
        	
        	st.executeUpdate("DROP TABLE IF EXISTS " + unions_next_rank);
        	st.executeUpdate("CREATE TABLE " + unions_next_rank 
    	    + " (set_key BIGINT DEFAULT 0, "
    	    + "set_name VARCHAR(20) DEFAULT NULL, "
    	    + "capacity INT DEFAULT NULL, " 
    	    + "availability INT DEFAULT NULL, " 
    	    + "goal INT DEFAULT 0, "
    	    + "PRIMARY KEY(set_key)) ENGINE=MEMORY");
        	
			st.executeUpdate("DROP TABLE IF EXISTS " + temp_unions);
			st.executeUpdate("CREATE /*TEMPORARY*/ TABLE " + temp_unions
		    	    + " (set_key BIGINT DEFAULT 0, "
		    	    + "set_name VARCHAR(20) DEFAULT NULL, "
		    	    + "capacity INT DEFAULT NULL, " 
		    	    + "availability INT DEFAULT NULL, " 
		    	    + "goal INT DEFAULT 0, "
		    	    + "PRIMARY KEY(set_key)) ENGINE=MEMORY");		 
        	
        	st.executeUpdate("DROP TABLE IF EXISTS " + allocation_ledger);
        	st.executeUpdate("CREATE TABLE " + allocation_ledger
            	    + " (set_key BIGINT DEFAULT NULL, "
            	    + "set_name VARCHAR(20) DEFAULT NULL, "
            	    + "capacity INT DEFAULT NULL, " 
            	    + "availability INT DEFAULT NULL, " 
            	    + "advertiserID  VARCHAR(80) DEFAULT NULL, "
            	    + "goal INT DEFAULT 0, "
            	    + "alloc_key VARCHAR(40) DEFAULT '', "
            	    + "PRIMARY KEY(alloc_key))");
        	
        	st.executeUpdate("DROP TABLE IF EXISTS " + result_serving);
        	st.executeUpdate("DROP TABLE IF EXISTS " + result_serving_copy);
        	
        	
        	st.executeUpdate("DROP PROCEDURE IF EXISTS PopulateRankWithNumbers");
        	st.executeUpdate("CREATE PROCEDURE PopulateRankWithNumbers() "
        			+ "BEGIN "
        			+ " UPDATE " + unions_next_rank + ", "
        			+ " (SELECT "
        			+ "    set_key, "
        			+ "    SUM(capacity) as capacity, "
        			+ "    SUM(availability) as availability "
        			+ "  FROM ("
        			+ "   SELECT unr1.set_key, ri.count as capacity, ri.count as availability "
        			+ "    FROM " + unions_next_rank + " unr1"
        			+ "    JOIN " + raw_inventory + " ri "
        			+ "    ON unr1.set_key & ri.basesets != 0 "
        			+ "    WHERE unr1.capacity is NULL "
        			+ "    ) blownUp "
        			+ "  GROUP BY set_key"
        			+ " ) comp "
        			+ " SET " + unions_next_rank + ".capacity = comp.capacity, "
        			+   unions_next_rank + ".availability = comp.availability "
        			+ " WHERE " + unions_next_rank + ".set_key = comp.set_key; "
        			+ " END "
        			);
        	        	
        	st.executeUpdate("DROP FUNCTION IF EXISTS BookItemsFromIS");
        	st.executeUpdate("CREATE FUNCTION BookItemsFromIS(iset BIGINT, amount INT) "
        			+ "RETURNS BOOLEAN "
        			+ "DETERMINISTIC "
        			+ "READS SQL DATA "
        			+ "BEGIN "
        			+ "    DECLARE cnt INT; "
        			+ "    SELECT availability INTO cnt FROM " + structured_data_base + " WHERE set_key_is = iset; "
        			+ "    IF cnt >= amount AND amount > 0 "
        			+ "    THEN "
        			+ "     UPDATE " + structured_data_base
        			+ "     SET availability=availability-amount, goal=goal+amount "
        			+ "     WHERE set_key_is = iset; "
        			+ "     RETURN TRUE; "
        			+ "    ELSE "
        			+ "     RETURN FALSE; "
        			+ "    END IF; "
        			+ "END "
        			);
        	
        	st.executeUpdate("DROP PROCEDURE IF EXISTS GetItemsFromSD");
        	st.executeUpdate("CREATE PROCEDURE GetItemsFromSD(IN iset BIGINT, IN amount INT, OUT result BOOLEAN) "
        			+ "BEGIN "
        			+ "IF BookItemsFromIS(iset, amount) "
        			+ "   THEN "
        			+ "     UPDATE " + structured_data_inc
        			+ "        SET availability = availability - amount "
        			+ "        WHERE (set_key & iset) = iset; "
        			+ "     SELECT TRUE INTO result; "
         			+ "   ELSE "
        			+ "     SELECT FALSE INTO result; "
        			+ "   END IF; "
        			+ "END "
        			);				
       			
        	st.executeUpdate("DROP PROCEDURE IF EXISTS CleanUpSD");
        	st.executeUpdate("CREATE PROCEDURE CleanUpSD() "
        			+ "BEGIN "
 					+ "     DELETE sd1 FROM " + structured_data_inc + " sd1 "
					+ "		INNER JOIN " + structured_data_inc + " sd2 "
					+ "          ON sd2.set_key > sd1.set_key "
					+ "          AND sd2.set_key & sd1.set_key = sd1.set_key "
					+ "          AND sd1.availability >= sd2.availability; "
        			+ "END "
        			);				
					
        	st.executeUpdate("DROP PROCEDURE IF EXISTS UpdateBaseData");
        	st.executeUpdate("CREATE PROCEDURE UpdateBaseData() "
        			+ "BEGIN "
        			+ "     UPDATE " + structured_data_base + " , " + structured_data_inc
        			+ "     SET " + structured_data_base + ".availability = LEAST(" + structured_data_base + ".availability, " + structured_data_inc + ".availability) "
        			+ "     WHERE " + structured_data_inc + ".set_key & " + structured_data_base + ".set_key_is = " + structured_data_base + ".set_key_is; "
        			+ "END "
        			);
        	st.executeUpdate("REPLACE INTO " + inventory_status + " VALUES(1, '" + Status.clean.name() + "')");
        }
    }
      
    public boolean hasData() throws SQLException
    {
		// Do the tables exist?
        try (Statement st = con.createStatement())
        {
        	java.sql.ResultSet rs = st.executeQuery("SELECT count(*) FROM " + structured_data_base);
    //    	java.sql.ResultSet rs = st.executeQuery("SELECT 1 FROM " + structured_data_base + " LIMIT 1");
	//    	java.sql.ResultSet rs = con.getMetaData().getTables(null, null, customer_name + "_raw_inventory_ex", null);
	        if (!rs.next())
	        {
	        	return false;
	        }
	        else if (rs.getLong(1) == 0)
	        {
	        	return false;
	        }
	        else
	        {
	        	return true;
	        }
        }
        catch (CommunicationsException ex)
        {
        	log.severe(customer_name + " : " + ex.getMessage());
        	return false;
        }
    }

    public String getStatus() throws SQLException // TODO: do we really need it?
    {
        try (Statement st = con.createStatement())
        {
        	ResultSet rs = st.executeQuery("SELECT * FROM " + inventory_status);
        	if (rs.next())
        	{
        		return rs.getString(2);
        	}
        	else
        	{
        		return Status.clean.name();
        	}
        }
    }
    
    boolean isLocked() throws SQLException // TODO: remove later
    {
		// Do the tables exist?
        try (Statement st = con.createStatement())
        {
        	boolean rs = st.execute("SELECT count(*) FROM " + structured_data_base);
        	return !rs;
        }
        catch (SQLException ex)
        {
        	return true;
        }
    }
       
    void unlock() throws SQLException // TODO: remove later
    {
        try (Statement st = con.createStatement())
        {
        	st.executeQuery("UNLOCK TABLES");
        	log.info(customer_name + " : databases unlocked");
       }
    }
    
    public void loadinvalid() throws SQLException
    {
        try (Statement st = con.createStatement())
        {
        	st.executeUpdate("REPLACE INTO " + inventory_status + " VALUES(1, '" + Status.loadinprogress.name() + "')");
        	log.info(customer_name + " : status invalidated");
        }
    }
    
    public void inconsitent() throws SQLException
    {
        try (Statement st = con.createStatement())
        {
        	st.executeUpdate("REPLACE INTO " + inventory_status + " VALUES(1, '" + Status.inconsitent.name() + "')");
        	log.info(customer_name + " : status inconsitent");
        }
    }

    public void wrongFile() throws SQLException
    {
        try (Statement st = con.createStatement())
        {
        	st.executeUpdate("REPLACE INTO " + inventory_status + " VALUES(1, '" + Status.wrongfile.name() + "')");
        	log.severe(customer_name + " : status set to wrong file");
        }
    }
    
    public void noData() throws SQLException
    {
        try (Statement st = con.createStatement())
        {
        	st.executeUpdate("REPLACE INTO " + inventory_status + " VALUES(1, '" + Status.nodata.name() + "')");
        	log.severe(customer_name + " : status set to no data for segments in the inventory");
        }
    }
    
    public void manySegmens() throws SQLException
    {
        try (Statement st = con.createStatement())
        {
        	st.executeUpdate("REPLACE INTO " + inventory_status + " VALUES(1, '" + Status.manysegmens.name() + "')");
        	log.severe(customer_name + " : status set to too many segmens in the inventory");
        }
    }
    
    public void highOverlap() throws SQLException
    {
        try (Statement st = con.createStatement())
        {
        	st.executeUpdate("REPLACE INTO " + inventory_status + " VALUES(1, '" + Status.highoverlap.name() + "')");
        	log.severe(customer_name + " : status set to too high segmens overlap");
        }
    }
   
    public void noSegments() throws SQLException
    {
        try (Statement st = con.createStatement())
        {
        	st.executeUpdate("REPLACE INTO " + inventory_status + " VALUES(1, '" + Status.nosegments.name() + "')");
        	log.severe(customer_name + " : status set no segmens in the inventory");
        }
    }
    
    public void unknownError() throws SQLException
    {
        try (Statement st = con.createStatement())
        {
        	st.executeUpdate("REPLACE INTO " + inventory_status + " VALUES(1, '" + Status.unknown.name() + "')");
        	log.severe(customer_name + " : status set to unknown error");
        }
    }

    public void loadstarted() throws SQLException
    {
        try (Statement st = con.createStatement())
        {
        	st.executeUpdate("REPLACE INTO " + inventory_status + " VALUES(1, '" + Status.loadstarted.name() + "')");
        	log.info(customer_name + " : status set load started");
        }
    }
    
    public void loaded() throws SQLException
    {
		try (Statement st = con.createStatement()) 
		{
			st.executeUpdate(" REPLACE INTO " + inventory_status + " VALUES(1, '" + Status.loaded.name() + "');");      			
        	log.info(customer_name + " : status set to loaded");
		}
	}

       
    public void toomuchdata() throws SQLException
    {
        try (Statement st = con.createStatement())
        {
        	st.executeUpdate("REPLACE INTO " + inventory_status + " VALUES(1, '" + Status.toomuchdata.name() + "')");
        	log.severe(customer_name + " : status set to too much data");
        }
    }

    public boolean isLoadInProgress() throws SQLException
    {
    	if (Status.valueOf(getStatus()) == Status.loadinprogress)
    		return true;
    	else
    		return false;
    }
    
    public boolean isSomethngWrong() throws SQLException
    {
    	switch (Status.valueOf(getStatus()))
    	{
    	case wrongfile:
    	case highoverlap:
    	case manysegmens:
    	case nosegments:
    	case nodata:
    	case toomuchdata:
    	case unknown: 
    		return true;
    	default:
    		return false;
    	}
//    	if (Status.valueOf(getStatus()) == Status.wrongfile)
//    		return true;
//    	else
//    		return false;
    }
    
    public boolean isClean() throws SQLException
    {
    	if (Status.valueOf(getStatus()) == Status.clean)
    		return true;
    	else
    		return false;
    	
    }

    public boolean isLoaded() throws SQLException
    {
    	if (Status.valueOf(getStatus()) == Status.loaded)
    		return true;
    	else
    		return false;
    }
    
    public boolean isLoadStarted() throws SQLException
    {
    	if (Status.valueOf(getStatus()) == Status.loadstarted)
    		return true;
    	else
    		return false;
    }
   
    public boolean loadDynamic(ReadableByteChannel readChannel, boolean reloadable) throws IOException, SQLException, ClassNotFoundException, InterruptedException
    {
		Calendar starting = new GregorianCalendar();
		Long startTime = starting.getTimeInMillis();

		if (!isLoadInProgress())
		{
		clear();
		loadstarted();
    	//convert json input to InventroryData object
		InventroryData inventorydata = null;
		try 
		{
			inventorydata= mapper.readValue(Channels.newInputStream(readChannel), InventroryData.class);
		}
		catch (JsonParseException | JsonMappingException ex)
		{
			log.severe(customer_name + " : There was an excetption " + ex.getMessage());
			wrongFile();
			return true;
		}
		if (inventorydata.getSegments().length > BITMAP_MAX_SIZE)
		{
			log.severe(customer_name + " : There are " + String.valueOf(inventorydata.getSegments().length) + " (more than bitmap can handle " + String.valueOf(BITMAP_MAX_SIZE) + ") inventory sets in " + readChannel.toString());
			manySegmens();
			return true;
		}
		// Create inventory sets data. TODO: write into DB from the start
		HashMap<BitSet, BaseSet> base_sets = new HashMap<BitSet, BaseSet>();			
		int highBit = 0;
		int bitmap_size = inventorydata.getSegments().length;
		for (segment is : inventorydata.getSegments())
		{
			log.info(customer_name + " processing segment " + is.getName() + " with criteria " + is.getCriteria().toString());
			if (is.getName() == null)
			{
				log.severe(customer_name + " : segment has invalid name " + is.getName());
				wrongFile();
				return true;
			}
			boolean match_found = false;
			// check if the set differs from existing sets
			for (BaseSet bs1 : base_sets.values())
			{
				if (criteria.equals(bs1.getCriteria(), is.getCriteria()))
				{
					match_found = true;
					log.warning(customer_name + " " + is.getName() + " repeats " + bs1.getname() + " with criteria: " + bs1.getCriteria().toString());
					break;
				}
			}
			if (match_found)
				continue; // skip repeated set
			
			BaseSet tmp = new BaseSet(bitmap_size);
			tmp.setkey(highBit);
			tmp.setname(is.getName());
			tmp.setCriteria(is.getCriteria());
			base_sets.put(tmp.getkey(), tmp);
			highBit++;
			log.info(customer_name + " added set " + tmp.getname() + " with criteria " + tmp.getCriteria().toString());
		}
		if (highBit == 0)
		{
			noSegments();
			return true;
		}			
		
		// Create segments' raw data. TODO: write into DB from the start
		HashMap<BitSet, BaseSegement> base_segments = new HashMap<BitSet, BaseSegement>();
		for (opportunity opp : inventorydata.getOpportunities())
		{
			boolean match_found = false;
			BaseSegement tmp = new BaseSegement();
			tmp.setCriteria(opp.getcriteria());
			
			for (BaseSet bs1 : base_sets.values())
			{					
				if (criteria.matches(bs1.getCriteria(), tmp.getCriteria()))
				{
					tmp.getkey().or(bs1.getkey());
					match_found = true;
					if (tmp.getkey().cardinality() > CARDINALITY_LIMIT)
					{
						// overlap exceeds half of allowed number of inventory sets
						highOverlap();
						return true;
					}
				}
			}
			if (match_found) 
			{
				int capacity = opp.getCount();
				BaseSegement existing = null;
				if ((existing = base_segments.get(tmp.getkey())) != null)
				{
					capacity += existing.getcapacity();
				}
				tmp.setcapacity(capacity);
				base_segments.put(tmp.getkey(), tmp);
			}
		}
		if (base_segments.isEmpty())
		{
			noData();
			return true;
		}

		//
		// Populate all tables 
		//
        // populate structured data with inventory sets
        try (Statement st = con.createStatement())
        {
        	st.executeUpdate("DROP TABLE IF EXISTS " + structured_data_base);
        	st.executeUpdate("CREATE TABLE " + structured_data_base 
        	+ " (set_key_is BIGINT DEFAULT 0, " // one bit identifying inventory set
		    + "set_key BIGINT DEFAULT NULL, "
		    + "set_name VARCHAR(20) DEFAULT NULL, "
		    + "capacity INT DEFAULT NULL, "
		    + "availability INT DEFAULT NULL, " 
//		    + "private_availability INT DEFAULT NULL, " 
		    + "goal INT DEFAULT 0, "
		    + "criteria VARCHAR(200) DEFAULT NULL, "
		    + "PRIMARY KEY(set_key_is))");        	
        }
        try (PreparedStatement insertStatement = con.prepareStatement("INSERT IGNORE INTO "  + structured_data_base 
        		+ " SET set_key = ?, set_name = ?, set_key_is = ?, criteria = ?"))
        {
	        for (BaseSet bs1 : base_sets.values()) {
	         	insertStatement.setLong(1, bs1.getKeyBin()[0]);
	            insertStatement.setString(2, bs1.getname());
	        	insertStatement.setLong(3, bs1.getKeyBin()[0]);
	        	if (bs1.getCriteria() == null)
	        	{
		        	insertStatement.setString(4, "");	        		
	        	}
	        	else
	        	{
	        		insertStatement.setString(4, bs1.getCriteria().toString());
	        	}	            
	        	insertStatement.execute();
	        }
        }
        
        try (Statement st = con.createStatement())
        {
	    	st.executeUpdate("DROP TABLE IF EXISTS " + raw_inventory);
	    	st.executeUpdate("CREATE TABLE " + raw_inventory 
	    	+ " (basesets BIGINT NOT NULL, "
	        + "count INT NOT NULL, "
	        + "criteria VARCHAR(200) DEFAULT NULL, "
	        + "weight BIGINT DEFAULT 0)");
        }
        // populate raw data with inventory sets' bitmaps
        try (PreparedStatement insertStatement = con.prepareStatement("INSERT INTO "  + raw_inventory 
        		+ " SET basesets = ?, count = ?, criteria = ?, weight = ? ON DUPLICATE KEY UPDATE count = VALUES(count) + count" ))
        {
        	log.info(customer_name + " : INSERT INTO "  + raw_inventory);
	        for (BaseSegement bs1 : base_segments.values()) {
	        	insertStatement.setLong(1, bs1.getKeyBin()[0]);
	        	insertStatement.setInt(2, bs1.getcapacity());
	        	if (bs1.getCriteria() == null)
	        	{
		        	insertStatement.setString(3, "");	        		
	        	}
	        	else
	        	{
	        		insertStatement.setString(3, bs1.getCriteria().toString());
	        	}
	        	insertStatement.setLong(4, 0);
	            insertStatement.execute();
	        }
        }
                
        // update raw inventory with weights
        try (PreparedStatement st = con.prepareStatement("SELECT @n:=0"))
        {
        	st.execute();
        }
        
        try (PreparedStatement st = con.prepareStatement("UPDATE " + raw_inventory
        		+ " SET weight = @n := @n + " + raw_inventory + ".count"))
        {
        	log.info(customer_name + " : UPDATE " + raw_inventory);
        	st.execute();
        }

        try (Statement st = con.createStatement())
        {
            // adds capacities and availabilities to structured base segments       	
        	st.executeUpdate("UPDATE "
        			+ structured_data_base
        			+ ", (SELECT set_key, SUM(" + raw_inventory + ".count) AS capacity, SUM(" + raw_inventory + ".count) AS availability FROM "
        			+ structured_data_base
        			+ " JOIN " 
        			+ raw_inventory 
        			+ " ON set_key & " + raw_inventory + ".basesets != 0 "
        			+ " GROUP BY set_key) comp "
        			+ " SET " + structured_data_base + ".capacity = comp.capacity, "
        			+ structured_data_base + ".availability = comp.availability "
        			+ " WHERE " + structured_data_base + ".set_key = comp.set_key");
        	log.info(customer_name + " : UPDATE " + structured_data_base);        	
        }
        
        //
        // Build necessary unions only, layer by layer
        //
        // first layer in union_next_rank table
        // this is done once so later we can pick up on task re-launch with unions_next_rank at any step
//		String queryString = "INSERT IGNORE INTO " + structured_data_inc + "\n"
//		+ "SELECT ri1.basesets, NULL, " + "SUM(ri2.count), SUM(ri2.count), 0 \n"
//		+ " FROM " + raw_inventory + " ri1 \n"
//		+ "	JOIN " + raw_inventory + " ri2\n"
//		+ " ON ri1.basesets & ri2.basesets > 0 AND ri1.basesets >= ri2.basesets \n"
//		+ "GROUP BY ri1.basesets \n"
//		;
        try (Statement st = con.createStatement())
        {
        	st.executeUpdate("INSERT INTO " + unions_next_rank
    			+ " SELECT set_key, set_name, capacity, availability, goal FROM " 
    			+ structured_data_base 
    			+ " WHERE capacity IS NOT NULL");
	        	log.info(customer_name + " :  INSERT INTO " + unions_next_rank);
//        	st.executeUpdate(queryString);
	    }
		}
		else
		{
			// TODO: bail out because another user could be doing the loading. Currently we rely on UI to prevent the interference.
			// For now if it is another launch of the task just keep going
			log.warning(customer_name + " : Loading was the process, while we tried again, could happend when the task restarted");
		}
		
		// We can restart from here 
		loadinvalid();
        
        ResultSet rs = null;
		try (Statement st = con.createStatement()) 
		{
			rs = st.executeQuery("SELECT count(*) FROM " + raw_inventory);
			if (rs.next()) {
				if (rs.getInt(1) == 0)
				{
					log.severe(customer_name + " : no data");
					rs.close();
					st.close();
					return false;
				}
			}
		}
		
		log.info(customer_name + " : Starting filling up the tables");
		
		if (!AdjustInventory(unions_next_rank, reloadable, startTime))
			return false;

    	// remove unneeded nodes
    	try (CallableStatement callStatement = con.prepareCall("{call " + BWdb + customer_name + ".CleanUpSD()}"))
    	{
    		callStatement.executeUpdate();
    	}
    	// UpdateBaseData
    	try (CallableStatement callStatement = con.prepareCall("{call " + BWdb + customer_name + ".UpdateBaseData()}"))
    	{
    		// UpdateBaseData can fail because of 5 msec limit on GAE connection
    		callStatement.executeUpdate();
    	}   	
    	catch (CommunicationsException ex)
    	{
    		log.severe(customer_name + " : GetItemsFrom caused an exception " + ex.getMessage());
    		// Reconnect back because the exception closed the connection.
			// timeoutHandler.reconnect();
     	}    	

    	// Validate the data in DB
        loaded();

		return true;
	}
    
    boolean AdjustInventory(String start_data, boolean reloadable, Long startTime) throws SQLException, ClassNotFoundException, InterruptedException
    {
		int iteration = 0;
		int insert_size = 0;
		int cnt = 0;
		int cnt_updated = 0;
    	String queryString;
        ResultSet rs = null;
        boolean keep_going = true;
    	
        do 
		{
        	// making sure we are not affected by task timeout
            Calendar currentTime = new GregorianCalendar();
            Long interval = currentTime.getTimeInMillis() - startTime;
//            if (reloadable && !(SystemProperty.environment.value() == SystemProperty.Environment.Value.Production) && 
//            		interval >= RESTART_INTERVAL)
//            {
//            	log.severe(customer_name + " : Restarting the loading inventory after " + interval.toString() + " msec. to avoid timeout");
//            	rs.close();
//            	return false;
//            }	            	
        	
//          build new layer with unions of higher rank  
    		try (Statement st = con.createStatement()) {
    			// save the previous layer
				st.executeUpdate("TRUNCATE " + unions_last_rank);
				st.executeUpdate("INSERT INTO " + unions_last_rank + " SELECT * FROM " + start_data);
				st.executeUpdate("TRUNCATE " + unions_next_rank);
				// build new layer with unions of higher rank 
				queryString = "INSERT IGNORE INTO " + unions_next_rank + "\n"
				+ "SELECT un.set_key, NULL AS set_name, un.capacity, un.capacity - " + "SUM(" + structured_data_base + ".goal) AS availability,\n"
				+ "SUM(" + structured_data_base + ".goal) as goal\n"
				+ "FROM (\n"
    			+ " SELECT \n"
    			+ "    set_key, \n"
    			+ "    SUM(capacity) as capacity \n"
    			+ " FROM (\n"
    			+ "  SELECT *, " + raw_inventory + ".count as capacity " 
    			+ "  FROM (SELECT ds.set_key FROM (\n"    			
    			+ "    SELECT DISTINCT " + structured_data_base + ".set_key | " + unions_last_rank + ".set_key as set_key \n"
    			+ "	   FROM " + unions_last_rank + "\n"
    			+ "    JOIN " + structured_data_base + "\n"
    			+ "	   JOIN " + raw_inventory + "\n"
     			+ "         ON  " + structured_data_base + ".set_key & " + raw_inventory + ".basesets != 0 \n"
    			+ "         AND " + unions_last_rank + ".set_key & " + raw_inventory + ".basesets != 0 \n"
    			+ "         AND " + structured_data_base + ".set_key | " + unions_last_rank + ".set_key > " + unions_last_rank + ".set_key) ds \n"
       			+ "	   LEFT OUTER JOIN " + structured_data_inc + "\n"
    			+ "         ON ds.set_key & " + structured_data_inc + ".set_key = ds.set_key \n"
				+ "         WHERE " + structured_data_inc + ".set_key IS NULL \n" // make sure we don't have its superset already
    			+ "   ) un_sk \n"
    			+ "   JOIN " + raw_inventory 
    			+ "   ON un_sk.set_key & " + raw_inventory + ".basesets != 0 \n"
    			+ " ) un_r\n"
    			+ " GROUP BY set_key) un\n"
    			+ "JOIN " + structured_data_base + " \n"
    			+ "ON " + structured_data_base + ".set_key & un.set_key != 0 \n"
    			+ "GROUP BY un.set_key, un.capacity \n"
    			;
				log.info(customer_name + " : iteration = " +  String.valueOf(iteration++) + " INSERT INTO unions_next_rank");
				st.executeUpdate(queryString);
				// make sure we don't have them or their supersets already
				
				rs = st.executeQuery("SELECT COUNT(*) FROM " + unions_next_rank);
				if (rs.next())
					insert_size = rs.getInt(1);
				if (insert_size != 0)
//					break; // no more unions
				{				
				log.info(customer_name + " : size of " + unions_next_rank + " = " + String.valueOf(insert_size));

			
				// ---
				// for all supersets that has the same capacity as the subset:
				//		drop the subset and add superset
				// 		repeat
				// ---
				// check for superset has the same availability as the subset
				st.executeUpdate("DROP TABLE IF EXISTS " + ex_inc_unions);
				queryString = "CREATE /*TEMPORARY*/ TABLE " + ex_inc_unions + " AS SELECT \n" // TODO: make the table temporary
				+ unions_last_rank + ".set_key as l_key, " + unions_next_rank + ".set_key as n_key, " + unions_next_rank + ".capacity, " + unions_next_rank + ".availability \n"
				+ " FROM " + unions_last_rank + "\n"
    			+ " JOIN " + unions_next_rank + "\n"
    			+ "      ON " + unions_last_rank + ".set_key & " + unions_next_rank + ".set_key = " + unions_last_rank + ".set_key \n"
    			+ "      AND " + unions_last_rank + ".availability = " + unions_next_rank + ".availability \n"
    			+ "      AND " + unions_next_rank + ".set_key > " + unions_last_rank + ".set_key";
				int row_cnt = st.executeUpdate(queryString);
				if (row_cnt > 0)
				{					
					// find highest supersets that of the same availability
					st.executeUpdate("DROP TABLE IF EXISTS " + ex_inc_unions1);
					queryString = "CREATE /*TEMPORARY*/ TABLE " + ex_inc_unions1 + " AS SELECT \n"
					+ ex_inc_unions + ".l_key, " 
					+ " BIT_OR(" + ex_inc_unions + ".n_key) AS n_key, "
					+ ex_inc_unions + ".availability, \n" // TODO: correct for SUM(goal)
					+ ex_inc_unions + ".capacity \n"
					+ " FROM " + ex_inc_unions 
					+ " GROUP BY " + ex_inc_unions + ".l_key, " + ex_inc_unions + ".availability, " + ex_inc_unions + ".capacity \n"
					;
					row_cnt = st.executeUpdate(queryString);							
							
					// keep only rows with availability lower than in next rank
					st.executeUpdate("TRUNCATE " + temp_unions);
					queryString = "INSERT /*IGNORE*/ INTO " + temp_unions + " SELECT " 
					+ unions_last_rank + ".set_key, "
					+ unions_last_rank + ".set_name, "
					+ unions_last_rank + ".capacity, " 
					+ unions_last_rank + ".availability, " 
					+ unions_last_rank + ".goal "
					+ " FROM " + unions_last_rank + "\n"
	    			+ " LEFT OUTER JOIN " + ex_inc_unions + "\n"
	    			+ "      ON " + unions_last_rank + ".set_key = " + ex_inc_unions + ".l_key \n"
					+ "      WHERE " + ex_inc_unions + ".l_key IS NULL \n";
					st.executeUpdate(queryString);					
					rs = st.executeQuery("SELECT COUNT(*) FROM " + temp_unions);
					if (rs.next()) 
					{
						insert_size = rs.getInt(1);
					}
					log.info(customer_name + " : size of " + temp_unions + " = " + String.valueOf(insert_size));
					// Finalize for insertion into structured_data_inc
					st.executeUpdate("TRUNCATE " + unions_last_rank);
					if (insert_size > 0)
						st.executeUpdate("INSERT INTO " + unions_last_rank + " SELECT * FROM " + temp_unions);
					
					// recreate supersets only for sets of the same as their subsets  capacity
					// st.executeUpdate("TRUNCATE " + unions_next_rank);
					// delete all supersets but of the highest rank 
					queryString = "DELETE " + unions_next_rank + " FROM " + unions_next_rank 
					+ " JOIN " + ex_inc_unions1
					+ "\n ON " + ex_inc_unions1 + ".n_key & " + unions_next_rank + ".set_key = " + unions_next_rank + ".set_key";
					st.executeUpdate(queryString);
					// and add highest unions that of the same capacity ????
					queryString = "INSERT /*IGNORE*/ INTO " + unions_next_rank + " SELECT DISTINCT " 
					+ ex_inc_unions1 + ".n_key AS set_key, "
					+ "NULL AS set_name, "
					+ ex_inc_unions1 + ".capacity, " 
					+ ex_inc_unions1 + ".availability, " 
					+ " 0 AS goal " // TODO: change to SUM(goal)
					+ " FROM " + ex_inc_unions1 + "\n";
					st.executeUpdate(queryString);
//					rs = st.executeQuery("SELECT COUNT(*) FROM " + unions_next_rank);					
//					if (rs.next())
//					{
//						insert_size = rs.getInt(1);
//						log.info(customer_name + " : corrected size of " + unions_next_rank + " = " + String.valueOf(insert_size));	
//						if (insert_size == 0)
//							break;
//					}
				}
				}
				else {
					keep_going = false;
				}
				log.info(customer_name + " : INSERT INTO " + structured_data_inc);	   			
				st.executeUpdate(
				" INSERT IGNORE INTO " + structured_data_inc // we do need IGNORE, inserts should be of higher rank but they may be inserted before ??
    			+ "    SELECT * FROM " + unions_last_rank);
								
				rs = st.executeQuery("SELECT count(*) FROM " + structured_data_inc);
				if (rs.next()) {
					cnt_updated = rs.getInt(1);
				}
				log.info(customer_name + " : cnt=" + String.valueOf(cnt) + ", cnt_updated=" + String.valueOf(cnt_updated));				
			}
			catch (CommunicationsException ex)
			{
				log.severe(customer_name + " : loadDynamic thrown " + ex.getMessage());
	    		// Reconnect back because the exception closed the connection.
				timeoutHandler.reconnect();
			}
//			catch (java.sql.SQLException ex) // TODO: do the handling in the caller
//			{
//				log.severe(customer_name + " : loadDynamic INSERT /*IGNORE*/ INTO " + unions_next_rank + " thrown " + ex.getMessage());
//				unknownError();
//				rs.close();
//				return true;
//			}
    		start_data = unions_next_rank; // after the first pass switch back to unions_next_rank
		} while (keep_going);
        
        // update base table with keys of supersets of the same availability
		try (Statement st = con.createStatement()) 
		{
			st.executeUpdate("UPDATE " + structured_data_base + "," + structured_data_inc
			+ " SET " + structured_data_base + ".set_key = " + structured_data_inc + ".set_key "
			+ " WHERE " + structured_data_base + ".set_key & " + structured_data_inc + ".set_key = " + structured_data_base + ".set_key "
			+ " AND " + structured_data_base + ".set_key < " + structured_data_inc + ".set_key "
			+ " AND " + structured_data_base + ".availability = " + structured_data_inc + ".availability; ");
			log.info(customer_name + " : UPDATE " + structured_data_base + " with keys of new inserts of the same availability");
		}
		catch (CommunicationsException ex)
		{
			log.severe(customer_name + " : UPDATE structured_data_base, structured_data_inc thrown " + ex.getMessage());
    		// Reconnect back because the exception closed the connection.
			timeoutHandler.reconnect();
		}
    	return true;
    }
    
    public boolean GetItems(String set_name, String advertiserID, int amount) throws SQLException, ClassNotFoundException, InterruptedException
    {
    	if (amount <= 0)
    	{
			log.severe(customer_name + " : wrong allocation = " + Integer.toString(amount));
			return false;
    	}
    	if (advertiserID.length() == 0)
    	{
    		// replace advertiser ID with time-stamp
    		advertiserID = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss").format(new Date());
    	}
    	
    	long set_key_is = 0;
    	int capacity = 0;
    	int availability = 0;
    	int structured_data_size = 0;
    	try (Statement statement = con.createStatement())
    	{
    		ResultSet rs = statement.executeQuery("SELECT set_key_is, capacity, availability FROM structured_data_base WHERE set_name = '" + set_name + "'");
	        if (rs.next())
	        {
	    		set_key_is = rs.getLong(1);
	    		capacity = rs.getInt(2);
	    		availability = rs.getInt(3);
	        }
	        else
	        {
	        	log.severe(customer_name + " : " + set_name + " set wasn't found");
	        	return false;
	        }
	        rs = statement.executeQuery("SELECT COUNT(*) FROM " + structured_data_inc);
	        if (rs.next())
	        {
	        	structured_data_size = rs.getInt(1);
	        }
    	}
    	
    	log.info(customer_name + " : Trying allocate for set_key_is=" + set_key_is + " amount=" + amount 
    			+ " while " + structured_data_inc + " size=" + Integer.toString(structured_data_size));    	
    	try (CallableStatement callStatement = con.prepareCall("{call " + BWdb + customer_name + ".GetItemsFromSD(?, ?, ?)}"))
    	{
	     	boolean returnValue = false;
			callStatement.setLong(1, set_key_is);
			callStatement.setInt(2, amount);
			callStatement.registerOutParameter(3, Types.BOOLEAN);
    		// GetItemsFromSD can fail because of 5 msec limit on GAE connection
    		callStatement.executeUpdate();
    		returnValue = callStatement.getBoolean(3);
    		if (!returnValue)
    		{
    			log.severe(customer_name + " : GetItemsFromSD failed for " + Long.toString(set_key_is) + " trying to allocate " + Integer.toString(amount));
    			callStatement.close();
     			return false; // TODO: we probably need some diagnostic for UI
    		}
    	}
    	// update structured_data_inc table
//		Calendar starting = new GregorianCalendar();
//		Long startTime = starting.getTimeInMillis();
////    	AdjustInventory(structured_data_inc, false, startTime);
    	// remove unneeded nodes
    	try (CallableStatement callStatement = con.prepareCall("{call " + BWdb + customer_name + ".CleanUpSD()}"))
    	{
    		callStatement.executeUpdate();
    	}
    	// UpdateBaseData
    	try (CallableStatement callStatement = con.prepareCall("{call " + BWdb + customer_name + ".UpdateBaseData()}"))
    	{
    		// UpdateBaseData can fail because of 5 msec limit on GAE connection
    		callStatement.executeUpdate();
    	}   	
    	catch (CommunicationsException ex)
    	{
    		log.severe(customer_name + " : GetItemsFrom caused an exception " + ex.getMessage());
    		// Reconnect back because the exception closed the connection.
			// timeoutHandler.reconnect();
    		return false;
    	}    	
    	log.info(customer_name + " : Allocation for set_key_is=" + set_key_is + " amount=" + amount + " was completed");
    	
    	// TODO: update union_next_rank table
    	
    	try (PreparedStatement statement = con.prepareStatement(
    	"INSERT INTO " + allocation_ledger + " (set_key, set_name, capacity, availability, advertiserID, goal, alloc_key) VALUES ('"
		    	+ String.valueOf(set_key_is) + "','" 
		    	+ set_name + "','"
		    	+ String.valueOf(capacity) + "','" 
		    	+ String.valueOf(availability) + "','" 
		    	+ advertiserID + "','" 
		    	+ String.valueOf(amount)  + "','"
		    	+ advertiserID + " | " + set_name
		    	+ "') ON DUPLICATE KEY UPDATE goal = VALUES(goal) + goal" ))
    	{
    		statement.executeUpdate();
    	}
    	catch (CommunicationsException ex)
    	{
    		log.severe(customer_name + " : INSERT INTO allocation_ledger caused an exception " + ex.getMessage());
    		// Reconnect back because the exception closed the connection.
    		return false;
    	}
    	log.info(customer_name + " : Allocated "
    	    	+ String.valueOf(set_key_is) + "','" 
    	    	+ set_name + "','"
    	    	+ String.valueOf(capacity) + "','" 
    	    	+ String.valueOf(availability) + "','" 
    	    	+ advertiserID + "','" 
    	    	+ String.valueOf(amount)
    			);
    	return true;
    }
    
    private static long getComplexity(HashMap<BitSet, BaseSegement> base_segments, int bitmap_size)
    {
    	Set<BitSet> base_seg_set = base_segments.keySet();    	
    	Iterator<BitSet> bs_it = base_seg_set.iterator();
    	long complexity = 0;    	
    	BitSet bs_and = new BitSet(bitmap_size);
    	BitSet bs_or = new BitSet(bitmap_size);
    	bs_and.set(0, bitmap_size);
        while (bs_it.hasNext()) 
        {
        	BitSet bs = bs_it.next();
        	bs_and.and(bs); // find bits always set to 1
        	bs_or.or(bs);   // find bits always set to 0
        }
        bs_and.xor(bs_or);  // find all varying bits
        bs_it = base_seg_set.iterator();
        while (bs_it.hasNext()) 
        {
        	BitSet bs = bs_it.next();
        	bs.and(bs_and);
        	complexity += bs.cardinality(); // accumulate differences
        }
    	return complexity;
    }
    
    @Override
    public void close() throws SQLException
    {
    	if(con!=null && !con.isClosed())
    	{
    		con.close();
    	}
    }
}
