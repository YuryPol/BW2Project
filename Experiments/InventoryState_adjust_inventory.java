package com.bwing.invmanage2;

import java.io.IOException;
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
import java.util.BitSet;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
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
    public static final String raw_inventory_aggregated = "raw_inventory_full";
    static final String structured_data_inc ="structured_data_inc";
    static final String structured_data_base = "structured_data_base";
    static final String unions_last_rank = "unions_last_rank";
    static final String unions_next_rank = "unions_next_rank";
    public static final String allocation_ledger = "allocation_protocol";
    public static final String result_serving = "result_serving";
    public static final String result_serving_copy = "result_serving_copy";
    static final String inventory_status = "inventory_status";
 
	static final public int BITMAP_SIZE = 40; // max = 64;
	static final public int CARDINALITY_LIMIT = 4;
	static final public int BASE_SETS_OVERLAPS_LIMIT = 100;
	private static final long RESTART_INTERVAL = 600000 - 10000; // less than 10 minutes
	
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
			url = "jdbc:mysql://localhost:3306?user=root&password=IraAnna12";
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
    
    public InventoryState(String name, boolean auto) throws ClassNotFoundException, SQLException
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
        	+ "', PRIMARY KEY(fake_key))");

        	// create raw_inventory table to fill up by impressions' counts
        	st.executeUpdate("DROP TABLE IF EXISTS " + raw_inventory);
        	st.executeUpdate("CREATE TABLE " + raw_inventory 
        	+ " (basesets BIGINT NOT NULL, "
            + "count INT NOT NULL, "
            + "criteria VARCHAR(200) DEFAULT NULL, "
            + "weight BIGINT DEFAULT 0)");

        	// create structured data table
        	st.executeUpdate("DROP TABLE IF EXISTS " + structured_data_inc);
        	st.executeUpdate("CREATE TABLE " +  structured_data_inc
    	    + " (set_key BIGINT DEFAULT 0, "
    	    + "set_name VARCHAR(20) DEFAULT NULL, "
    	    + "capacity INT DEFAULT NULL, " 
    	    + "availability INT DEFAULT NULL, " 
    	    + "goal INT DEFAULT 0, "
    	    + "PRIMARY KEY(set_key))");        	
        	
        	// create inventory sets table (schema was changed so for backward compatibility we create it on the fly)
//        	st.executeUpdate("DROP TABLE IF EXISTS " + structured_data_base);
//        	st.executeUpdate("CREATE TABLE " + structured_data_base 
//        	+ " (set_key_is BIGINT DEFAULT 0, "
//		    + "set_key BIGINT DEFAULT NULL, "
//		    + "set_name VARCHAR(20) DEFAULT NULL, "
//		    + "capacity INT DEFAULT NULL, "
//		    + "availability INT DEFAULT NULL, " 
//		    + "private_availability INT DEFAULT NULL, " 
//		    + "goal INT DEFAULT 0, "
//		    + "criteria VARCHAR(200) DEFAULT NULL, "
//		    + "PRIMARY KEY(set_key_is))");

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
        	st.executeUpdate("CREATE PROCEDURE GetItemsFromSD(IN iset BIGINT, IN amount INT, OUT result BOOLEAN) "
        			+ "BEGIN "
        			+ "IF BookItemsFromIS(iset, amount) "
        			+ "   THEN "
        			+ "     UPDATE structured_data_inc "
        			+ "        SET availability = availability - amount "
        			+ "        WHERE (set_key & iset) = iset; "
        			// this is a first attempt to clean the structured data. It takes too much time
//        			+ "     DELETE FROM structured_data_inc WHERE set_key = ANY ( "
//        			+ "       SELECT set_key FROM ( "
//        			+ "          SELECT sd1.set_key "
//        			+ "          FROM structured_data_inc sd1 JOIN structured_data_inc sd2 "
//        			+ "          ON sd2.set_key > sd1.set_key "
//        			+ "          AND sd2.set_key & sd1.set_key = sd1.set_key "
//        			+ "          AND sd1.availability >= sd2.availability) AS stmp "
//        			+ "       ); "
					// This is a second attempt to clean the structured data. It takes too much time
					+ "     DELETE sd1 FROM structured_data_inc sd1 "
					+ "		INNER JOIN structured_data_inc sd2 "
					+ "          ON sd2.set_key > sd1.set_key "
					+ "          AND sd2.set_key & sd1.set_key = sd1.set_key "
					+ "          AND sd1.availability >= sd2.availability; "
        			+ "     UPDATE structured_data_base sdbR, structured_data_inc sd "
        			+ "     SET sdbR.availability = LEAST(sdbR.availability, sd.availability) "
        			+ "     WHERE sd.set_key & sdbR.set_key_is = sdbR.set_key_is; "
        			+ "     SELECT TRUE INTO result; "
        			+ "   ELSE "
        			+ "     SELECT FALSE INTO result; "
        			+ "   END IF; "
        			+ "END "
        			);
//        	st.executeUpdate("REPLACE INTO " + inventory_status + " VALUES(1, '" + Status.loaded.name() + "')");
        }
    }
    
    public void clear() throws SQLException
    {
    	// Truncate all tables
        try (Statement st = con.createStatement())
        {
        	st.executeUpdate("TRUNCATE " + raw_inventory);
        	st.executeUpdate("TRUNCATE " + structured_data_inc);
        	st.executeUpdate("TRUNCATE " + structured_data_base);
        	st.executeUpdate("TRUNCATE " + unions_last_rank);
        	st.executeUpdate("TRUNCATE " + unions_next_rank);
        	st.executeUpdate("TRUNCATE " + allocation_ledger);
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
    
    void lock() throws SQLException // TODO: remove later
    {
        try (Statement st = con.createStatement())
        {
        	st.execute("LOCK TABLES "
        			+ raw_inventory + " WRITE, "
        			+ structured_data_inc + " WRITE, "
        			+ structured_data_base + " WRITE, "
        			+ unions_last_rank + " WRITE, "
        			+ unions_next_rank + " WRITE, "
        			+ unions_next_rank  + " AS unr1 WRITE, "
        			+ structured_data_base + " AS sdbW WRITE, " 
        			+ structured_data_base + " AS sdbR READ, "
        			+ raw_inventory + " AS ri READ, "
        			+ allocation_ledger + " WRITE, "
        			+ inventory_status + " WRITE "
      			);
        	log.warning(customer_name + " : tables were locked");
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
   
    public boolean loadDynamic(ReadableByteChannel readChannel, boolean reloadable) throws JsonParseException, JsonMappingException, IOException, SQLException, ClassNotFoundException, InterruptedException
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
		catch (Exception ex)
		{
			log.severe(customer_name + " : There was an excetption " + ex.getMessage());
			wrongFile();
			return true;
		}
		if (inventorydata.getSegments().length > BITMAP_SIZE)
		{
			log.severe(customer_name + " : There are " + String.valueOf(inventorydata.getSegments().length) + " (more than bitmap can handle " + String.valueOf(BITMAP_SIZE) + ") inventory sets in " + readChannel.toString());
			manySegmens();
			return true;
		}
		// Create inventory sets data. TODO: write into DB from the start
		HashMap<BitSet, BaseSet> base_sets = new HashMap<BitSet, BaseSet>();			
		int highBit = 0;
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
			// check if the set already exists
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
			
			BaseSet tmp = new BaseSet(BITMAP_SIZE);
			tmp.setkey(highBit);
			tmp.setname(is.getName());
			tmp.setCriteria(is.getCriteria());
			// tmp.setPrivateAvailablity(1);
			base_sets.put(tmp.getkey(), tmp);
			highBit++;
			log.info(customer_name + " added set " + tmp.getname() + " with criteria " + tmp.getCriteria().toString());
		}
		if (highBit == 0)
		{
			noSegments();
			return true;
		}			
		
		int max_cardinality = 0;
		// Create segments' raw data.
		HashMap<BitSet, BaseSegement> base_segments = new HashMap<BitSet, BaseSegement>();
		HashMap<BitSet, BaseSegement> base_segments_to_disribute = new HashMap<BitSet, BaseSegement>();
		HashMap<BitSet, BaseSegement> base_segments_private = new HashMap<BitSet, BaseSegement>();
		for (opportunity opp : inventorydata.getOpportunities())
		{
			// Walk through all opportunities, attribute them to base_sets and combine into base segments
			boolean match_found = false;
			boolean opp_cardinality_overlimit = false;
			BaseSegement tmp = new BaseSegement(BITMAP_SIZE);
			tmp.setCriteria(opp.getcriteria());
			
			for (BaseSet bs1 : base_sets.values())
			{					
				if (bs1.getCriteria() == null || bs1.getCriteria().size() == 0)
				{
					tmp.getkey().or(bs1.getkey());
					match_found = true; // segment with any criteria matches base set with no criteria
				}
				else if (criteria.matches(bs1.getCriteria(), tmp.getCriteria()))
				{
					if (bs1.getkey().cardinality() == 1 && bs1.getCriteria().equals(tmp.getCriteria()))
					{
						bs1.setPrivateAvailablity(tmp.getcapacity());
					}
					tmp.getkey().or(bs1.getkey());
					match_found = true;
					if (tmp.getkey().cardinality() > CARDINALITY_LIMIT)
					{
						// overlap exceeds the limit (half of allowed number of inventory sets for now)
						max_cardinality = tmp.getkey().cardinality();
						opp_cardinality_overlimit = true;
					}
				}
			}
			if (match_found) 
			{
				// put it either into shared, private or distributed collection
				HashMap<BitSet, BaseSegement> workingSet;
				int capacity = opp.getCount();
				if (tmp.getkey().cardinality() == 1)
				{
					// segment matching single base set	
					workingSet = base_segments_private;
				}
				else if (opp_cardinality_overlimit)
				{
					workingSet = base_segments_to_disribute;
				}
				else
				{
					workingSet = base_segments;					
				}				
				BaseSegement existing = null;
				if ((existing = workingSet.get(tmp.getkey())) != null)
				{
					// If the segment existed already update it
					capacity += existing.getcapacity();
				}
				tmp.setcapacity(capacity);
				workingSet.put(tmp.getkey(), tmp);
			}
		}
		
		if (base_segments.isEmpty()
				&& base_segments_to_disribute.isEmpty()
				&& base_segments_private.isEmpty())
		{
			noData();
			return true;
		}
		
		// concatenate all base segments into full raw data table to use for simulation
		HashMap<BitSet, BaseSegement> base_segments_aggregated = new HashMap<BitSet, BaseSegement>();
		base_segments_aggregated.putAll(base_segments_private);
		base_segments_aggregated.putAll(base_segments_to_disribute);
		base_segments_aggregated.putAll(base_segments);		
    	// create raw_inventory table to fill up by impressions' counts
        try (Statement st = con.createStatement())	//TODO: replace with memory-cache
        {
	    	st.executeUpdate("DROP TABLE IF EXISTS " + raw_inventory_aggregated);
	    	st.executeUpdate("CREATE TABLE " + raw_inventory_aggregated 
	    	+ " (basesets BIGINT NOT NULL, "
	        + "count INT NOT NULL, "
	        + "criteria VARCHAR(200) DEFAULT NULL, "
	        + "weight BIGINT DEFAULT 0)");
        }
        // populate aggregated raw data with inventory sets' bitmaps
        try (PreparedStatement insertStatement = con.prepareStatement("INSERT INTO "  + raw_inventory_aggregated 
        		+ " SET basesets = ?, count = ?, criteria = ?, weight = ? ON DUPLICATE KEY UPDATE count = VALUES(count) + count" ))
        {
        	log.info(customer_name + " : INSERT INTO "  + raw_inventory_aggregated);
	        for (BaseSegement bs1 : base_segments_aggregated.values()) {
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

		if (base_segments_to_disribute.size() > 0)
		{
			log.warning(customer_name + " : segments overlap cardinality is too high, " + Integer.toString(max_cardinality) + " for file " + readChannel.toString() + " compacting raw data");
			// compact raw data by distributing high cardinality segments unless we must to keep them
			for (BaseSegement to_dist : base_segments_to_disribute.values())
			{
				// find all base_segments included in base_segments_to_disribute, but not included into included
				HashSet<BitSet> sets_to_increase = new HashSet<BitSet>();
				HashSet<BitSet> subsets_to_remove = new HashSet<BitSet>();
				// for starter find all
				boolean include_found = false;
				for (BaseSegement to_add : base_segments.values())
				{
					if (to_dist.contains(to_add)) // contained base_segments
					{
						if (!sets_to_increase.add(to_add.getkey())) {log.severe(customer_name + " : has a duplicate BaseSegement " + to_add.getkey().toString());} // sanity check
						include_found = true;
					}
				}
				if (!include_found)
				{
					// keep high cardinality segment as it is the only one in existence 
					base_segments.put(to_dist.getkey(), to_dist);
					continue;
				}
				// now remove included into included base_segments because we want to distribute only to highest rank nodes
				BitSet superset = null;
				BitSet subset = null;
				for (Iterator<BitSet> superset_i = sets_to_increase.iterator(); superset_i.hasNext();)
				{
					superset = superset_i.next();
					for (Iterator<BitSet> subset_i = sets_to_increase.iterator(); subset_i.hasNext();)
					{
						subset = subset_i.next();
						if (BaseSegement.key_contains(superset, subset) && !superset.equals(subset))
						{
							subsets_to_remove.add(subset);
						}
					}
				}
				sets_to_increase.removeAll(subsets_to_remove);
				
				// Now find all base_segments_private included in base_segments_to_disribute and not included in base_segments
				// so we don't do distribution for the later 
				for (BaseSegement to_add : base_segments_private.values())
				{
					boolean private_include_found = false;
					if (to_dist.contains(to_add)) 
					{
						// contained in base_segments_to_disribute
						for (BaseSegement to_check : base_segments.values())
						{
							if (to_check.contains(to_add)) 
							{
								// contained in base_segments
								private_include_found = true;
								break; // don't add
							}
						}
						if (!private_include_found)
						{
							sets_to_increase.add(to_add.getkey());
						}
					}					
				}
				
				// count total capacity of base_segments to distribute between
				int total_capacity = 0;
				for (BitSet to_add : sets_to_increase)
				{
					total_capacity += base_segments.get(to_add).getcapacity();
				}
				// distribute high cardinality segments between others proportional to the later capacity
				for (BitSet to_add : sets_to_increase)
				{
					base_segments.get(to_add).addcapacity(
							base_segments.get(to_add).getcapacity() / total_capacity
							* base_segments_to_disribute.get(to_dist.getkey()).getcapacity());
				}
			}
		}
		
		// Make sure that base segments collection size does not exceed the limit
		if (base_segments.size() > BASE_SETS_OVERLAPS_LIMIT)
		{
			log.warning(customer_name + " : number of overlapping segments is too high, " + Integer.toString(base_segments.size()) + " for file " + readChannel.toString() + " compacting raw data");
			// Distribute lower capacities, so sort by their values first
			LinkedHashMap<BitSet, BaseSegement> sorted_base_segments = sortByComparator(base_segments, false);
			base_segments.clear();
			base_segments_to_disribute.clear();
			int ind = 0;
			// and now distribute lower capacities
			for (Entry<BitSet, BaseSegement> entry : sorted_base_segments.entrySet())
			{				
				// add to distribution
				if (ind >= BASE_SETS_OVERLAPS_LIMIT)
				{
					base_segments_to_disribute.put(entry.getKey(), entry.getValue());
					log.info(customer_name + " : to distiribute segment " + entry.getKey().toString() + " capacity " + Integer.toString(entry.getValue().getcapacity()));
				}
				else
				{
					base_segments.put(entry.getKey(), entry.getValue());
					log.info(customer_name + " : to keep segment " + entry.getKey().toString() + " capacity " + Integer.toString(entry.getValue().getcapacity()));
				}
				ind++;
			}
			
			for (BaseSegement to_dist : base_segments_to_disribute.values())
			{
				// find all base_segments included in base_segments_to_disribute, but not included into included
				HashSet<BitSet> sets_to_increase = new HashSet<BitSet>();
				HashSet<BitSet> subsets_remove = new HashSet<BitSet>();
				// for starter find all
				boolean include_found = false;
				for (BaseSegement to_add : base_segments.values())
				{
					if (to_dist.contains(to_add)) // contained base_segments
					{
						if (!sets_to_increase.add(to_add.getkey())) {log.severe(customer_name + " : has a duplicate BaseSegement " + to_add.getkey().toString());} // sanity check
						include_found = true;
					}
				}
				if (!include_found)
				{
					// keep high cardinality segment as it is the only one in existence 
					base_segments.put(to_dist.getkey(), to_dist);
					continue;
				}
				// now remove included into included base_segments
				BitSet superset = null;
				BitSet subset = null;
				for (Iterator<BitSet> superset_i = sets_to_increase.iterator(); superset_i.hasNext();)
				{
					superset = superset_i.next();
					for (Iterator<BitSet> subset_i = sets_to_increase.iterator(); subset_i.hasNext();)
					{
						subset = subset_i.next();
						if (BaseSegement.key_contains(superset, subset) && !superset.equals(subset))
						{
							subsets_remove.add(subset);
						}
					}
				}
				sets_to_increase.removeAll(subsets_remove);
				// find all base_segments_private included in base_segments_to_disribute and not included in base_segments
				// so we don't do distribution for their supersets 
				for (BaseSegement to_add : base_segments_private.values())
				{
					boolean private_include_found = false;
					if (to_dist.contains(to_add)) 
					{
						// contained in base_segments_to_disribute
						for (BaseSegement to_check : base_segments.values())
						{
							if (to_check.contains(to_add)) 
							{
								// contained in base_segments
								private_include_found = true;
								break; // don't add
							}
						}
						if (!private_include_found)
						{
							sets_to_increase.add(to_add.getkey());
						}
					}					
				}
				
				// count total capacity of base_segments to distribute between
				int total_capacity = 0;
				for (BitSet to_add : sets_to_increase)
				{
					total_capacity += base_segments.get(to_add).getcapacity();
				}
				// distribute high cardinality segments between others proportional to their capacity
				for (BitSet to_add : sets_to_increase)
				{
					base_segments.get(to_add).addcapacity(
							base_segments.get(to_add).getcapacity() 
							* base_segments_to_disribute.get(to_dist.getkey()).getcapacity() / total_capacity);
				}
			}
					
		}
		

		//
		// Populate all tables 
		//
        // populate structured data with inventory sets
        try (Statement st = con.createStatement())
        {
        	// recreate the table here for backward compatibility as we changed the schema 
        	st.executeUpdate("DROP TABLE IF EXISTS " + structured_data_base);
        	st.executeUpdate("CREATE TABLE " + structured_data_base 
        	+ " (set_key_is BIGINT DEFAULT 0, "
		    + "set_key BIGINT DEFAULT NULL, "
		    + "set_name VARCHAR(20) DEFAULT NULL, "
		    + "capacity INT DEFAULT NULL, "
		    + "availability INT DEFAULT NULL, " 
		    + "private_availability INT DEFAULT NULL, " 
		    + "goal INT DEFAULT 0, "
		    + "criteria VARCHAR(200) DEFAULT NULL, "
		    + "PRIMARY KEY(set_key_is))");
        }
        try (PreparedStatement insertStatement = con.prepareStatement("INSERT IGNORE INTO "  + structured_data_base 
        		+ " SET set_key = ?, set_name = ?, set_key_is = ?, criteria = ?, private_availability = ?"))
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
	        	insertStatement.setInt(5, bs1.getPrivateAvailablity());
	        	insertStatement.execute();
	        }
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
        
        // start union_next_rank table
        try (PreparedStatement insertStatement = con.prepareStatement("INSERT INTO " + unions_next_rank
    			+ " SELECT set_key, set_name, capacity, availability, goal FROM " 
    			+ structured_data_base 
    			+ " WHERE capacity IS NOT NULL"))
	        {
	        	log.info(customer_name + " :  INSERT INTO " + unions_next_rank);
	        	insertStatement.execute();        	
	        }
		}
		
		// We can restart from here 
		loadinvalid();
        
        int cnt = 0;
        int cnt_updated = 0;
        ResultSet rs = null;
		try (Statement st = con.createStatement()) 
		{
//			do {
			rs = st.executeQuery("SELECT count(*) FROM " + raw_inventory);
			if (rs.next()) {
				cnt = rs.getInt(1);
			}
		}
		
		log.info(customer_name + " : Starting filling up the tables");
        for (int ind = 0; ; ind++) 
		{
            Calendar currentTime = new GregorianCalendar();
            Long interval = currentTime.getTimeInMillis() - startTime;
            if (reloadable && !(SystemProperty.environment.value() == SystemProperty.Environment.Value.Production) && 
            		interval >= RESTART_INTERVAL)
            {
            	log.warning(customer_name + " : Restarting the loading inventory after " + interval.toString() + " msec.");
            	rs.close();
            	return false;
            }	            	
        	
    		try (Statement st = con.createStatement()) {
				st.executeUpdate("TRUNCATE " + unions_last_rank);
				st.executeUpdate("INSERT INTO " + unions_last_rank + " SELECT * FROM " + unions_next_rank);
				st.executeUpdate("TRUNCATE " + unions_next_rank);
				// adds unions of higher rank for nodes to of structured_data_inc
				String addUnions = " INSERT /*IGNORE*/ INTO " + unions_next_rank
    			+ "    SELECT " + structured_data_base + ".set_key_is | " + unions_last_rank + ".set_key, NULL, NULL, NULL, 0 "
    			+ "	   FROM " + unions_last_rank
    			+ "    JOIN " + structured_data_base
    			+ "	   JOIN " + raw_inventory
    			+ "    ON  (" + structured_data_base + ".set_key_is & " + raw_inventory + ".basesets != 0) "
    			+ "        AND (" + unions_last_rank + ".set_key & " + raw_inventory + ".basesets) != 0 "
    			+ "        AND (" + structured_data_base + ".set_key_is | " + unions_last_rank + ".set_key) != " + unions_last_rank + ".set_key "
    			+ "        AND " + structured_data_base + ".set_key_is | " + unions_last_rank + ".set_key > " + structured_data_base + ".set_key_is"
    			+ "    GROUP BY " + structured_data_base + ".set_key_is | " + unions_last_rank + ".set_key;";
				st.executeUpdate(addUnions);
				log.info(customer_name + " : iteration = " +  String.valueOf(ind) + " INSERT /*IGNORE*/ INTO unions_next_rank");
			}
    		catch (CommunicationsException ex)
    		{			
				log.severe(customer_name + " : loadDynamic INSERT /*IGNORE*/ INTO " + unions_next_rank + " thrown " + ex.getMessage());
	    		// Reconnect back because the exception closed the connection.
				timeoutHandler.reconnect();
			}
			catch (java.sql.SQLException ex)
			{
				log.severe(customer_name + " : loadDynamic INSERT /*IGNORE*/ INTO " + unions_next_rank + " thrown " + ex.getMessage());
				unknownError();
				rs.close();
				return true;
			}
			
			try (CallableStatement callStatement = con.prepareCall("{CALL PopulateRankWithNumbers}")) {
				log.info(customer_name + " : {call PopulateRankWithNumbers}");
				callStatement.executeUpdate();
			}
			catch (CommunicationsException ex)
			{
				log.severe(customer_name + " : loadDynamic PopulateRankWithNumbers thrown " + ex.getMessage());
	    		// Reconnect back because the exception closed the connection.
				timeoutHandler.reconnect();
			}
			// PopulateRankWithNumbers completed
			
			try (Statement st = con.createStatement()) {
				log.info(customer_name + " : DELETE FROM unions_last_rank what is to be replaced");
//				st.executeUpdate(
//					"DELETE FROM " + unions_last_rank
//    			+ "    WHERE EXISTS ("
//    			+ "        SELECT * "
//    			+ "        FROM " + unions_next_rank + " unr1"
//    			+ "        WHERE (" + unions_last_rank + ".set_key & unr1.set_key) = " + unions_last_rank + ".set_key "
//    			+ "        AND " + unions_last_rank + ".capacity = unr1.capacity)");
				
//				st.executeUpdate(
//					"DELETE QUICK " + unions_last_rank + " FROM " + unions_last_rank
//    			+ "        INNER JOIN " + unions_next_rank + " unr1"
//    			+ "        WHERE (" + unions_last_rank + ".set_key & unr1.set_key) = " + unions_last_rank + ".set_key "
//    			+ "        AND " + unions_last_rank + ".capacity = unr1.capacity");
				
				st.executeUpdate("DROP TABLE IF EXISTS toInsert");
				st.executeUpdate("CREATE TEMPORARY TABLE toInsert AS SELECT \n" 
				+ unions_last_rank + ".set_key, "
				+ unions_last_rank + ".set_name, "
				+ unions_last_rank + ".capacity, " 
				+ unions_last_rank + ".availability, " 
				+ unions_last_rank + ".goal "
				+ " FROM " + unions_last_rank + "\n"
    			+ " LEFT OUTER JOIN " + unions_next_rank + "\n"
    			+ "      ON " + unions_last_rank + ".set_key & " + unions_next_rank + ".set_key = " + unions_last_rank + ".set_key \n"
    			+ "      AND   " + unions_last_rank + ".capacity = " + unions_next_rank + ".capacity \n"
				+ "      WHERE " + unions_next_rank + ".set_key IS NULL \n");

			}
			catch (CommunicationsException ex)
			{
				log.severe(customer_name + " : loadDynamic DELETE FROM unions_last_rank thrown " + ex.getMessage());
	    		// Reconnect back because the exception closed the connection.
				timeoutHandler.reconnect();
			}
			// deletion unneeded nodes completed	
	   			
			try (Statement st = con.createStatement()) {
				log.info(customer_name + " : INSERT INTO " + structured_data_inc);
				st.executeUpdate(
				" INSERT /*IGNORE*/ INTO " + structured_data_inc
    			+ "    SELECT * FROM toInsert");
			}
			catch (CommunicationsException ex)
			{
				log.severe(customer_name + " : loadDynamic INSERT /*IGNORE*/ INTO structured_data_inc thrown " + ex.getMessage());
	    		// Reconnect back because the exception closed the connection.
				timeoutHandler.reconnect();
			}
    			
			try (Statement st = con.createStatement()) {
				rs = st.executeQuery("SELECT count(*) FROM " + structured_data_inc);
				if (rs.next()) {
					cnt_updated = rs.getInt(1);
				}
				log.info(customer_name + " : cnt=" + String.valueOf(cnt) + ", cnt_updated=" + String.valueOf(cnt_updated));
				rs = st.executeQuery("SELECT count(*) FROM " + unions_next_rank);
				if (rs.next()) {
					if (rs.getInt(1) == 0)
						break;
				}
			}
//			} while (cnt < cnt_updated);

			try (Statement st = con.createStatement()) 
			{
				st.executeUpdate("DELETE FROM " + structured_data_inc + " WHERE capacity IS NULL; ");
			}
			catch (CommunicationsException ex)
			{
				log.severe(customer_name + " : DELETE FROM structured_data_inc WHERE capacity IS NULL thrown " + ex.getMessage());
	    		// Reconnect back because the exception closed the connection.
				timeoutHandler.reconnect();
			}
			
			try (Statement st = con.createStatement()) 
			{
				st.executeUpdate("UPDATE " + structured_data_base + "," + structured_data_inc
				+ " SET " + structured_data_base + ".set_key = " + structured_data_inc + ".set_key "
				+ " WHERE " + structured_data_base + ".set_key_is & " + structured_data_inc + ".set_key = " + structured_data_base + ".set_key_is "
				+ " AND " + structured_data_base + ".capacity = " + structured_data_inc + ".capacity; ");
				log.info(customer_name + " : UPDATE " + structured_data_base + " with ne inserts");
			}
			catch (CommunicationsException ex)
			{
				log.severe(customer_name + " : UPDATE structured_data_base, structured_data_inc thrown " + ex.getMessage());
	    		// Reconnect back because the exception closed the connection.
				timeoutHandler.reconnect();
			}
		}

		// Validate the data in DB
        loaded();

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
    	int private_availability = 0;
    	int structured_data_size = 0;
    	try (Statement statement = con.createStatement())
    	{
    		ResultSet rs = statement.executeQuery("SELECT set_key_is, capacity, availability, private_availability FROM structured_data_base WHERE set_name = '" + set_name + "'");
	        if (rs.next())
	        {
	    		set_key_is = rs.getLong(1);
	    		capacity = rs.getInt(2);
	    		availability = rs.getInt(3);
	    		private_availability = rs.getInt(4);
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
    	
    	if (private_availability >= amount)
    	{
        	log.info(customer_name + " : Allocating for set_key_is=" + set_key_is + " amount=" + amount 
        			+ " from private availability");
			try (Statement st = con.createStatement()) 
			{
				st.executeUpdate("UPDATE " + structured_data_base + "SET private_availability = " + Integer.toString(private_availability - amount));
			}
			catch (CommunicationsException ex)
			{
				log.severe(customer_name + " : UPDATE structured_data_base, structured_data_inc thrown " + ex.getMessage());
				return false;
			}
			
    	}
    	else
    	{
    	if (private_availability > 0)
    	{
        	log.info(customer_name + " : Partially allocating for set_key_is=" + set_key_is + " amount=" + amount 
        			+ " from private availability");
			try (Statement st = con.createStatement()) 
			{
				st.executeUpdate("UPDATE " + structured_data_base + "SET private_availability = 0 ");
				amount -= private_availability;
			}
			catch (CommunicationsException ex)
			{
				log.severe(customer_name + " : UPDATE structured_data_base, structured_data_inc thrown " + ex.getMessage());
				return false;
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
    	catch (CommunicationsException ex)
    	{
    		log.severe(customer_name + " : GetItemsFromSD caused an exception " + ex.getMessage());
    		// Reconnect back because the exception closed the connection.
			// timeoutHandler.reconnect();
    		return false;
    	}
    	}
    	log.info(customer_name + " : Allocation for set_key_is=" + set_key_is + " amount=" + amount + " was completed");
    	
    	try (PreparedStatement statement = con.prepareStatement(
    	"INSERT INTO " + allocation_ledger + " (set_key, set_name, capacity, availability, advertiserID, goal, alloc_key) VALUES ('"
		    	+ String.valueOf(set_key_is) + "','" 
		    	+ set_name + "','"
		    	+ String.valueOf(capacity) + "','" 
		    	+ String.valueOf(availability + private_availability) + "','" 
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
    	    	+ String.valueOf(availability + private_availability) + "','" 
    	    	+ advertiserID + "','" 
    	    	+ String.valueOf(amount)
    			);
    	return true;
    }
    
    private void DistiributeSegments(HashMap<BitSet, BaseSegement> base_segments_to_disribute
    		, HashMap<BitSet, BaseSegement> base_segments, HashMap<BitSet, BaseSegement> base_segments_private)
    {
		for (BaseSegement to_dist : base_segments_to_disribute.values())
		{
			// find all base_segments included in base_segments_to_disribute, but not included into included
			HashSet<BitSet> sets_to_increase = new HashSet<BitSet>();
			HashSet<BitSet> subsets_remove = new HashSet<BitSet>();
			// for starter find all
			boolean include_found = false;
			for (BaseSegement to_add : base_segments.values())
			{
				if (to_dist.contains(to_add)) // contained base_segments
				{
					if (!sets_to_increase.add(to_add.getkey())) {log.severe(customer_name + " : has a duplicate shared BaseSegement " + to_add.getkey().toString());} // sanity check
					include_found = true;
				}
			}
			if (base_segments_private != null)
			{
				for (BaseSegement to_add : base_segments_private.values())
				{
					if (to_dist.contains(to_add)) // contained base_segments_private
					{
						if (!sets_to_increase.add(to_add.getkey())) {log.severe(customer_name + " : has a duplicate private BaseSegement " + to_add.getkey().toString());} // sanity check
						include_found = true;
					}
				}				
			}
			else if (!include_found)
			{
				// keep high cardinality segment as it is the only shared one in existence 
				base_segments.put(to_dist.getkey(), to_dist);
				continue;
			}
			// now remove included into included segments
			BitSet superset = null;
			BitSet subset = null;
			for (Iterator<BitSet> superset_i = sets_to_increase.iterator(); superset_i.hasNext();)
			{
				superset = superset_i.next();
				for (Iterator<BitSet> subset_i = sets_to_increase.iterator(); subset_i.hasNext();)
				{
					subset = subset_i.next();
					if (BaseSegement.key_contains(superset, subset) && !superset.equals(subset))
					{
						subsets_remove.add(subset);
					}
				}
			}
			sets_to_increase.removeAll(subsets_remove);
			// find all base_segments_private included in base_segments_to_disribute and not included in base_segments
			// so we don't do distribution for their supersets 
//			for (BaseSegement to_add : base_segments_private.values())
//			{
//				boolean private_include_found = false;
//				if (to_dist.contains(to_add)) 
//				{
//					// contained in base_segments_to_disribute
//					for (BaseSegement to_check : base_segments.values())
//					{
//						if (to_check.contains(to_add)) 
//						{
//							// contained in base_segments
//							private_include_found = true;
//							break; // don't add
//						}
//					}
//					if (!private_include_found)
//					{
//						sets_to_increase.add(to_add.getkey());
//					}
//				}					
//			}
			
			// count total capacity of base_segments to distribute between
			int total_capacity = 0;
			base_segments.values().addAll(base_segments_private.values()); // ditribute between shared and private
			for (BitSet to_add : sets_to_increase)
			{
				total_capacity += base_segments.get(to_add).getcapacity();
			}
			// distribute high cardinality segments between others proportional to their capacity
			for (BitSet to_add : sets_to_increase)
			{
				base_segments.get(to_add).addcapacity(
						base_segments.get(to_add).getcapacity() 
						* base_segments_to_disribute.get(to_dist.getkey()).getcapacity() / total_capacity);
			}
		}	    	
    }
    
    private static LinkedHashMap<BitSet, BaseSegement> sortByComparator(HashMap<BitSet, BaseSegement> unsortMap, final boolean order)
    {

        List<Entry<BitSet, BaseSegement>> list = new LinkedList<Entry<BitSet, BaseSegement>>(unsortMap.entrySet());

        // Sorting the list based on values
        Collections.sort(list, new Comparator<Entry<BitSet, BaseSegement>>()
        {
            public int compare(Entry<BitSet, BaseSegement> bs1,
                    Entry<BitSet, BaseSegement> bs2)
            {
                if (order)
                {
                    return bs1.getValue().compareTo(bs2.getValue());
                }
                else
                {
                    return bs2.getValue().compareTo(bs1.getValue());

                }
            }
        });

        // Maintaining insertion order with the help of LinkedList
        LinkedHashMap<BitSet, BaseSegement> sortedMap = new LinkedHashMap<BitSet, BaseSegement>();
        for (Entry<BitSet, BaseSegement> entry : list)
        {
            sortedMap.put(entry.getKey(), entry.getValue());
        }

        return sortedMap;
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
