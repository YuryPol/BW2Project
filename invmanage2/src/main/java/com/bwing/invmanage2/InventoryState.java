package com.bwing.invmanage2;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
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
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.appengine.api.utils.SystemProperty;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;
import com.mysql.jdbc.exceptions.jdbc4.CommunicationsException;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;

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
    private static final String temp_unions = "temp_unions";
    static final String inventory_status = "inventory_status";
 
    static public boolean DEBUG = false;
	static public int BITMAP_MAX_SIZE = 40; // max = 64;
	static public int CARDINALITY_LIMIT = 10;
	static public int INVENTORY_OVERLAP = 500;
	static public int BASE_SETS_OVERLAPS_LIMIT = 100;
	private static long RESTART_INTERVAL = 600000 - 10000; // less than 10 minutes
	private static int MAX_ROW_SIZE = 3000;
	private static double OVERLAP_FRACTION = 0.1;
	private static double CARDINALITY_TRESHHOLD = 0.67; // 2/3;
	
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
        // Load properties
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
        	+ "', PRIMARY KEY(fake_key))");

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
		HashMap<BitSet, BaseSegement> base_segments_private = new HashMap<BitSet, BaseSegement>();
		for (opportunity opp : inventorydata.getOpportunities())
		{
			// Walk through all opportunities, attribute them to base_sets and combine into base segments
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
						// overlap exceeds allowed number of inventory sets
						highOverlap();
						return true;
					}
				}
			}
			if (match_found) 
			{
				// put it either into shared, private or distributed collection
				HashMap<BitSet, BaseSegement> workingSet;
				int capacity = opp.getCount();
				if (capacity <= 0)
					continue; // ignore empty or invalid opportunities 
				
				if (tmp.getkey().cardinality() == 1)
				{
					// segment matching single base set	
					workingSet = base_segments_private;
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
		long complexity;
		if (base_segments.isEmpty()
				&& base_segments_private.isEmpty())
		{
			noData();
			return true;
		}
		else if ((complexity = getComplexity(base_segments, bitmap_size)) > INVENTORY_OVERLAP)
		{
			log.warning(customer_name + " : segments overlap is too high, complexity = " + complexity + ""
					+ ", base segments size = " + Integer.toString(base_segments.size()) + " for file " + readChannel.toString());
//			highOverlap();
//			return true;
			
			// Now we need reduce the complexity by distributing small segments
			// weight them
			int minCapacity = Integer.MAX_VALUE;
			for ( Entry<BitSet, BaseSegement> tmp : base_segments.entrySet()) {
				minCapacity = (tmp.getValue().getcapacity() < minCapacity) ? tmp.getValue().getcapacity() : minCapacity;
			}
			double weights[] = new double[base_segments.size()];
			int ind = 0;
			for ( Entry<BitSet, BaseSegement> tmp : base_segments.entrySet()) {
				tmp.getValue().setweight(minCapacity);
				weights[ind++] = tmp.getValue().getweight();
			}
			// find segments to distribute and re-pack the base sets
			Percentile percentile = new Percentile();
			percentile.setData(weights);
			double threshold = percentile.evaluate(25); // weight threshold for distribution
			log.info("percentile = " + threshold);
			ArrayList<BitSet> toRemove = new ArrayList<BitSet>();
			HashMap<BitSet, BaseSegement> base_segments_tmp = new HashMap<BitSet, BaseSegement>();
			int toDistCapacityTotal = 0;
			int distCapacityTotal = 0;
			for ( Entry<BitSet, BaseSegement> tmp : base_segments.entrySet()) {
				if (tmp.getValue().getweight() < threshold) {
					// list for removal
					toRemove.add(tmp.getKey());
					// and distribute to private segments
					int distCount = tmp.getKey().cardinality();
					int distCapacity = tmp.getValue().getcapacity();
					int index = 0;
					if (distCapacity / distCount <= 0)
						continue; // not enough to distribute
					toDistCapacityTotal += distCapacity;
					while ((index = tmp.getKey().nextSetBit(index)) != -1) {
						BitSet bs = new BitSet();
						bs.set(index);
						BaseSegement bsp = base_segments_private.get(bs);
						if (bsp == null) {
							// create new private segment
							bsp = new BaseSegement();
							bsp.setcapacity(distCapacity / distCount);
							bsp.setkeybit(index);
							base_segments_private.put(bsp.getkey(), bsp);
						}
						else {
							// add the segment to existing private segment
							bsp.addcapacity(distCapacity / distCount);
						}
						index++;
						distCapacityTotal += distCapacity / distCount;
					}
				}
				else {
					base_segments_tmp.put(tmp.getKey(), tmp.getValue());
				}
			}
			log.info("distributed = " + distCapacityTotal + " out of " + toDistCapacityTotal);
			base_segments.clear();
			base_segments = base_segments_tmp;
		}
		
		// now check complexity again
		ArrayList<HashMap<BitSet, BaseSegement>> base_segments_instances = new ArrayList<HashMap<BitSet, BaseSegement>>();
		if ((complexity = getComplexity(base_segments, bitmap_size)) > INVENTORY_OVERLAP) {
			log.warning(customer_name + " : after distribution of small segments the overlap is still too high, complexity = " + complexity + ""
					+ ", base segments size = " + Integer.toString(base_segments.size()) + " for file " + readChannel.toString());
			// break into clusters
			base_segments_instances = BuildClusters.getClusters(base_segments
					, (int) (complexity / INVENTORY_OVERLAP + 1), 10);
		}
		else {
			// go ahead with just one
			log.info(customer_name + " : after distribution of small segments the overlap is acceptable, complexity = " + complexity + ""
					+ ", base segments size = " + Integer.toString(base_segments.size()) + " for file " + readChannel.toString());
			base_segments_instances.add(base_segments);
		}
		
		// compact bit set
		int instance = 0;
		// to write visual representation 
		FileWriter fwTXT = new FileWriter("base_segments_instance." + instance + ".txt");
		BufferedWriter bwTXT = new BufferedWriter(fwTXT);
		bwTXT.write("Visualize instnaces as they are broken into clusters\n");
		for (HashMap<BitSet, BaseSegement>  base_segments_instance : base_segments_instances) {
			if ((complexity = getComplexity(base_segments_instance, bitmap_size)) > INVENTORY_OVERLAP) {
				double[]bcnts = new double[bitmap_size];;
				log.info(customer_name + " instance " + instance + " size of " + base_segments_instance.size() + " at the start");
				int bitsSetCnt = BaseSegement.getBitsCounts(base_segments_instance, bcnts, bwTXT);
				log.info(customer_name + " : " + bitsSetCnt + " bits were set in instance " + instance + " size of " + base_segments_instance.size() + " at the start");
				bwTXT.write(bitsSetCnt + " bits were set in instance " + instance + " at the start\n");
				// find threshold to clear bits
				ArrayList<Double> bcntsList = new ArrayList<Double>();
				for (double bcnt : bcnts) {
					if (bcnt > 0)
						bcntsList.add(bcnt);
				}
				Percentile percentile = new Percentile();
				double[] bcnts_values = Doubles.toArray(bcntsList);
				percentile.setData(bcnts_values);
				double threshold = percentile.evaluate(25); // threshold for compacting
				log.info(customer_name + " : threshold count for compacting = " + threshold);
				// create mask
				BitSet mask = new BitSet();
				int index = 0;
				for (double bcnt : bcnts) {
					if (bcnt > threshold)
						mask.set(index);
					index++;
				}

				// clear bits with low counts
				HashMap<BitSet, BaseSegement>  base_segments_instance_new = new HashMap<BitSet, BaseSegement>();
				for (Entry<BitSet, BaseSegement> bs : base_segments_instance.entrySet()) {
					bs.getKey().and(mask);
					bs.getValue().getkey().and(mask);
					if (base_segments_instance_new.putIfAbsent(bs.getKey(), bs.getValue()) != null) {
						base_segments_instance_new.get(bs.getKey()).addcapacity(bs.getValue().getcapacity());
					};
				}
				base_segments_instance = base_segments_instance_new;
				// Now visualize new instance
				bitsSetCnt = BaseSegement.getBitsCounts(base_segments_instance, bcnts, bwTXT);
				log.info(customer_name + " : " + bitsSetCnt + " bits were set in instance " + instance + " size of " + base_segments_instance.size() + " at the end");
				bwTXT.write(bitsSetCnt + " bits were set in instance " + instance + " at the end\n");
			}
			else {
				// go ahead without compacting
				log.info(customer_name + " : no need for compacting, complexity = " + complexity + ""
						+ ", base segments size = " + Integer.toString(base_segments.size()) + " for file " + readChannel.toString());
			}
			instance++;
		}
		bwTXT.close();
		fwTXT.close();

		//
		// Populate all tables 
		//
        // populate structured data with inventory sets
        try (Statement st = con.createStatement())
        {
        	// recreate the tables and procedures here for backward compatibility as we changed the schema 
        	st.executeUpdate("DROP TABLE IF EXISTS " + structured_data_base);
        	st.executeUpdate("CREATE TABLE " + structured_data_base 
        	+ " (set_key_is BIGINT DEFAULT 0, " // one bit identifying inventory set
		    + "set_key BIGINT DEFAULT NULL, "
		    + "set_name VARCHAR(20) DEFAULT NULL, "
		    + "capacity INT DEFAULT NULL, "
		    + "availability INT DEFAULT NULL, " 
		    + "private_availability INT DEFAULT NULL, " 
		    + "goal INT DEFAULT 0, "
		    + "criteria VARCHAR(200) DEFAULT NULL, "
		    + "PRIMARY KEY(set_key_is))");        	
        }
        
        // thrown The table 'unions_next_rank' is full
//     	if (unions_next_rank.size() > max_heap_table_size = 16777216)
//     	{
//     		do something ;
//     	}

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
	        	if (base_segments_private.get(bs1.getkey()) != null)
	        		insertStatement.setInt(5, base_segments_private.get(bs1.getkey()).getcapacity());
	        	else 
	        		insertStatement.setInt(5, 0);
	        	insertStatement.execute();
	        }
        }
        
    	// create raw_inventory table to fill up by impressions' counts
        try (Statement st = con.createStatement())
        {
	    	st.executeUpdate("DROP TABLE IF EXISTS " + raw_inventory);
	    	st.executeUpdate("CREATE TABLE " + raw_inventory 
	    	+ " (basesets BIGINT NOT NULL, "
	    	+ "basepart  BIGINT NOT NULL, "
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
        
        // start union_next_rank table
        try (Statement st = con.createStatement())
        {
        	st.executeUpdate("INSERT INTO " + unions_next_rank
    			+ " SELECT set_key, set_name, capacity, availability, goal FROM " 
    			+ structured_data_base 
    			+ " WHERE capacity IS NOT NULL");
	        	log.info(customer_name + " :  INSERT INTO " + unions_next_rank);
	        }
		}
		
		// We can restart from here 
		loadinvalid();
        
        int cnt = 0;
        int cnt_updated = 0;
        int cnt_basesets = 0;
        int insert_size = 0;
        ResultSet rs = null;
		try (Statement st = con.createStatement()) 
		{
			rs = st.executeQuery("SELECT count(*) FROM " + raw_inventory);
			if (rs.next()) {
				cnt = rs.getInt(1);
			}
			rs = st.executeQuery("SELECT count(*) FROM " + structured_data_base);
			if (rs.next()) {
				cnt_basesets = rs.getInt(1);
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
				// adds unions of higher rank for nodes to structured_data_inc
				String addUnions = " INSERT /*IGNORE*/ INTO " + unions_next_rank
    			+ " SELECT "
    			+ "    set_key, "
    			+ "    NULL as set_name, "
    			+ "    SUM(capacity) as capacity, "
    			+ "    SUM(availability) as availability, "
    			+ "    0 as goal "
    			+ " FROM ("
    			+ "  SELECT *, " + raw_inventory + ".count as capacity, " + raw_inventory + ".count as availability "
    			+ "  FROM ("    			
    			+ "    SELECT DISTINCT " + structured_data_base + ".set_key_is | " + unions_last_rank + ".set_key as set_key "
    			+ "	   FROM " + unions_last_rank
    			+ "    JOIN " + structured_data_base
    			+ "	   JOIN " + raw_inventory
    			+ "         ON  " + structured_data_base + ".set_key_is & " + raw_inventory + ".basesets != 0 "
    			+ "         AND " + unions_last_rank + ".set_key & " + raw_inventory + ".basesets != 0 "
    			+ "         AND " + structured_data_base + ".set_key_is | " + unions_last_rank + ".set_key > " + unions_last_rank + ".set_key "
    			+ "   ) un_sk "
    			+ "   JOIN " + raw_inventory 
    			+ "   ON un_sk.set_key & " + raw_inventory + ".basesets != 0 "
    			+ " ) un_r"
    			+ " GROUP BY set_key"
    			;
				log.info(customer_name + " : iteration = " +  String.valueOf(ind) + " INSERT /*IGNORE*/ INTO unions_next_rank");
				st.executeUpdate(addUnions);
				rs = st.executeQuery("SELECT COUNT(*) FROM " + unions_next_rank);
				if (rs.next()) {
					insert_size = rs.getInt(1);
				}
				log.info(customer_name + " : size of " + unions_next_rank + " = " + String.valueOf(insert_size));

				// if superset has the same capacity as the subset keep only the superset
				st.executeUpdate("DROP TABLE IF EXISTS " + temp_unions);
				st.executeUpdate("CREATE /*TEMPORARY*/ TABLE " + temp_unions + " AS SELECT \n" 
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
				// if sets in the superset don't overlap they won't make it in				
				rs = st.executeQuery("SELECT COUNT(*) FROM " + temp_unions);
				if (rs.next()) 
				{
					insert_size = rs.getInt(1);
				}
				log.info(customer_name + " : size of " + temp_unions + " = " + String.valueOf(insert_size));
				// copy to unions_last_rank
				st.executeUpdate("TRUNCATE " + unions_last_rank);
				st.executeUpdate("INSERT INTO " + unions_last_rank + " SELECT * FROM " + temp_unions);
				// deletion unneeded nodes completed	
				log.info(customer_name + " : INSERT INTO " + structured_data_inc);
				
				if (insert_size > MAX_ROW_SIZE && ind < cnt_basesets * CARDINALITY_TRESHHOLD)
				{
					//
					// Eliminate weakly overlapping unions from unions_next_rank, reduce capacities in unions_last_rank and remove unions from structured_data_inc accordingly
					// if there are too many rows to insert and we are still at the beginning 
					//
				}
    		}
			try (Statement st = con.createStatement()) {
				log.info(customer_name + " : INSERT INTO " + structured_data_inc);
				st.executeUpdate(
				" INSERT /*IGNORE*/ INTO " + structured_data_inc // we don't need IGNORE as inserts should be of higher rank
    			+ "    SELECT * FROM toInsert");
			}
			catch (CommunicationsException ex)
			{
				log.severe(customer_name + " : loadDynamic thrown " + ex.getMessage());
	    		// Reconnect back because the exception closed the connection.
				timeoutHandler.reconnect();
			}
			catch (java.sql.SQLException ex) // TODO: do the handling in the caller
			{
				log.severe(customer_name + " : loadDynamic INSERT /*IGNORE*/ INTO " + unions_next_rank + " thrown " + ex.getMessage());
				unknownError();
				rs.close();
				return true;
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
			
			try (Statement st = con.createStatement()) 
			{
				st.executeUpdate("UPDATE " + structured_data_base + "," + structured_data_inc
				+ " SET " + structured_data_base + ".set_key = " + structured_data_inc + ".set_key "
				+ " WHERE " + structured_data_base + ".set_key_is & " + structured_data_inc + ".set_key = " + structured_data_base + ".set_key_is "
				+ " AND " + structured_data_base + ".capacity = " + structured_data_inc + ".capacity; ");
				log.info(customer_name + " : UPDATE " + structured_data_base + " with keys of new inserts of the same capacity");
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
				st.executeUpdate("UPDATE " + structured_data_base + " SET private_availability = " + Integer.toString(private_availability - amount));
			}
			catch (CommunicationsException ex)
			{
				log.severe(customer_name + " : UPDATE structured_data_base, structured_data_inc thrown " + ex.getMessage());
				return false;
			}
			return true;
    	}
    	else if (private_availability > 0)
    	{
        	log.info(customer_name + " : Partially allocating for set_key_is=" + set_key_is + " amount=" + amount 
        			+ " from private availability");
			try (Statement st = con.createStatement()) 
			{
				st.executeUpdate("UPDATE " + structured_data_base + " SET private_availability = 0 ");
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
    
    // Returns Complexity of the base set
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
        	bs_and.and(bs); // find bits set to 1
        	bs_or.or(bs);   // find bits set to 0
        }
        bs_and.xor(bs_or);  // find all varying bits
    	
        bs_it = base_seg_set.iterator();
        while (bs_it.hasNext()) 
        {
        	BitSet bs = bs_it.next();
        	bs.xor(bs_and);
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
