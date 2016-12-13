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
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.ServletException;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.appengine.api.utils.SystemProperty;
import com.mysql.jdbc.exceptions.jdbc4.CommunicationsException;

import java.sql.ResultSet;
;

public class InventoryState implements AutoCloseable
{
	private enum Status {
		clean, loadstarted, loadinprogress, wrongfile, inconsitent, toomuchdata, loaded
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
    static final String inventory_status = "inventory_status";
 
	static final public int BITMAP_SIZE = 20; // max = 64;
	static final public int INVENTORY_OVERLAP = 250;
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
        			+ "    SELECT sdbR.set_key_is | unions_last_rank.set_key, NULL, NULL, NULL, 0 "
        			+ "	   FROM unions_last_rank "
        			+ "    JOIN structured_data_base sdbR "
        			+ "	   JOIN raw_inventory ri "
        			+ "    ON  (sdbR.set_key_is & ri.basesets != 0) "
        			+ "        AND (unions_last_rank.set_key & ri.basesets) != 0 "
        			+ "        AND (sdbR.set_key_is | unions_last_rank.set_key) != unions_last_rank.set_key "
        			+ "    GROUP BY sdbR.set_key_is | unions_last_rank.set_key;"
        			
        			+ " CALL PopulateRankWithNumbers;"
        			
        			+ " DELETE FROM structured_data_inc "
        			+ "    WHERE EXISTS ("
        			+ "        SELECT * "
        			+ "        FROM unions_next_rank unr1"
        			+ "        WHERE (structured_data_inc.set_key & unr1.set_key) = structured_data_inc.set_key "
        			+ "        AND structured_data_inc.capacity = unr1.capacity); "
        			
        			+ " INSERT /*IGNORE*/ INTO structured_data_inc "
        			+ "    SELECT * FROM unions_next_rank;"
        			
        			+ " SELECT count(*) INTO cnt_updated FROM structured_data_inc; "
        			
        			+ " UNTIL  (cnt = cnt_updated) "
        			
        			+ " END REPEAT; "
        			
        			+ " DELETE FROM structured_data_inc "
        			+ "    WHERE capacity IS NULL; "
        			
        			+ " UPDATE structured_data_base, structured_data_inc "
        			+ "    SET structured_data_base.set_key = structured_data_inc.set_key "
        			+ "    WHERE structured_data_base.set_key_is & structured_data_inc.set_key = structured_data_base.set_key_is "
        			+ "    AND structured_data_base.capacity = structured_data_inc.capacity; "
        			
        			// Validate the data in DB
                	+ " REPLACE INTO " + inventory_status + " VALUES(1, '" + Status.loaded.name() + "');"       			
        			
        			+ "END "
        			);
        	
        	st.executeUpdate("DROP PROCEDURE IF EXISTS AddUnionsDynamic"); //Add Unions Dynamically 
        	st.executeUpdate("CREATE PROCEDURE AddUnionsDynamic() "
        			+ " BEGIN "
        			+ " INSERT /*IGNORE*/ INTO " + unions_next_rank
        			+ "    SELECT sdbR.set_key_is | " + unions_last_rank + ".set_key, NULL, NULL, NULL, 0 "
        			+ "	   FROM " + unions_last_rank
        			+ "    JOIN " + structured_data_base + " sdbR "
        			+ "	   JOIN " + raw_inventory + " ri "
        			+ "    ON  (sdbR.set_key_is & ri.basesets != 0) "
        			+ "        AND (" + unions_last_rank + ".set_key & ri.basesets) != 0 "
        			+ "        AND (sdbR.set_key_is | " + unions_last_rank + ".set_key) != " + unions_last_rank + ".set_key "
        			+ "    WHERE BIT_COUNT(sdbR.set_key_is | " + unions_last_rank + ".set_key) > BIT_COUNT(sdbR.set_key_is)"
        			+ "    GROUP BY sdbR.set_key_is | " + unions_last_rank + ".set_key;"
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
//        		log.info("Status is " + rs.getString(2));
//        		return Status.valueOf(rs.getString(2));
        		return rs.getString(2);
        	}
        	else
        	{
        		return Status.loadinprogress.name();
        	}
        }
    }
    
    void lock() throws SQLException // TODO: remove later
    {
        try (Statement st = con.createStatement())
        {
        	boolean res = st.execute("LOCK TABLES "
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
        	log.info(customer_name + " : status set to wrong file");
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
       
    public void toomuchdata() throws SQLException
    {
        try (Statement st = con.createStatement())
        {
        	st.executeUpdate("REPLACE INTO " + inventory_status + " VALUES(1, '" + Status.toomuchdata.name() + "')");
        	log.info(customer_name + " : status set to too much data");
        }
    }

    public boolean isLoadInProgress() throws SQLException
    {
    	if (Status.valueOf(getStatus()) == Status.loadinprogress)
    		return true;
    	else
    		return false;
    }
    
    public boolean isWrongFile() throws SQLException
    {
    	if (Status.valueOf(getStatus()) == Status.wrongfile)
    		return true;
    	else
    		return false;
    }
    
    public boolean isTooMuchData() throws SQLException
    {
    	if (Status.valueOf(getStatus()) == Status.toomuchdata)
    		return true;
    	else
    		return false;
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
   
    public void load(ReadableByteChannel readChannel) throws JsonParseException, JsonMappingException, IOException, SQLException
    {
    	clear();
    	//convert json input to InventroryData object
		InventroryData inventorydata= mapper.readValue(Channels.newInputStream(readChannel), InventroryData.class);
		if (inventorydata.getSegments().length > BITMAP_SIZE)
		{
			log.severe(customer_name + " :  There are " + String.valueOf(inventorydata.getSegments().length) + " (more than allowed " + String.valueOf(BITMAP_SIZE) + ") inventory sets in " + readChannel.toString());
			wrongFile();
			return;
		}
		// Create inventory sets data. TODO: write into DB from the start
		HashMap<BitSet, BaseSet> base_sets = new HashMap<BitSet, BaseSet>();			
		int highBit = 0;
		for (segment is : inventorydata.getSegments())
		{
			boolean match_found = false;
			for (BaseSet bs1 : base_sets.values())
			{
/*				if (bs1.getCriteria() == null && is.getcriteria() == null)
				{
					match_found = true;
					break;
				}
				else if (bs1.getCriteria() == null)
				{
					// because is.criteria isn't null no need to compare
					continue;
				}
				else */
				if (criteria.equals(bs1.getCriteria(), is.getCriteria()))
				{
					match_found = true;
					break;
				}
			}
			if (match_found)
				continue; // skip repeated set
			
			BaseSet tmp = new BaseSet(BITMAP_SIZE);
			tmp.setkey(highBit);
			tmp.setname(is.getName());
			tmp.setCriteria(is.getCriteria());
			base_sets.put(tmp.getkey(), tmp);
			highBit++;
		}
		if (highBit == 0)
		{
			log.severe(customer_name + " :  no data in inventory sets in " + readChannel.toString());
			wrongFile();
			return;
		}			
		
		// Create segments' raw data. TODO: write into DB from the start
		HashMap<BitSet, BaseSegement> base_segments = new HashMap<BitSet, BaseSegement>();
		for (opportunity seg : inventorydata.getOpportunities())
		{
			boolean match_found = false;
			BaseSegement tmp = new BaseSegement(BITMAP_SIZE);
			tmp.setCriteria(seg.getcriteria());
			
			for (BaseSet bs1 : base_sets.values())
			{					
				if (bs1.getCriteria() == null && tmp.getCriteria() == null)
				{
					tmp.getkey().or(bs1.getkey());
					match_found = true;
				}
				else if (bs1.getCriteria() == null)
				{
					// because tmp.criteria isn't null no need to compare
					continue;
				}
				else if (criteria.matches(bs1.getCriteria(), tmp.getCriteria()))
				{
					tmp.getkey().or(bs1.getkey());
					match_found = true;
				}
			}
			if (match_found) 
			{
				int capacity = seg.getCount();
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
			log.severe(customer_name + " :  no data in segments " + readChannel.toString());
			wrongFile();
			return;
		}
		else if (base_segments.size() > INVENTORY_OVERLAP)
		{
			log.severe(customer_name + " : segments overlap is too high, " + Integer.toString(base_segments.size()) + " for file " + readChannel.toString());
			wrongFile();
			return;
		}

		//
		// Populate all tables 
		//
        // populate structured data with inventory sets
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
        
        // populate raw data with inventory sets' bitmaps
        try (PreparedStatement insertStatement = con.prepareStatement("INSERT INTO "  + raw_inventory 
        		+ " SET basesets = ?, count = ?, criteria = ?, weight = ? ON DUPLICATE KEY UPDATE count = VALUES(count) + count" ))
        {
        	log.info(customer_name + " :  INSERT INTO "  + raw_inventory);
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
        	log.info( customer_name + " : UPDATE " + raw_inventory);
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
        	log.info(customer_name + " : UPDATE " + structured_data_base);
        	
        	// populate inventory sets table
        	st.executeUpdate("INSERT INTO " 
        			+ structured_data_inc
        			+ " SELECT set_key, set_name, capacity, availability, goal FROM " 
        			+ structured_data_base 
        			+ " WHERE capacity IS NOT NULL");
        	log.info(customer_name + " : INSERT INTO " + structured_data_inc);
        }
        
        // start union_next_rank table
        try (PreparedStatement insertStatement = con.prepareStatement("INSERT INTO " + unions_next_rank
        		+ " SELECT * FROM " + structured_data_inc))
        {
        	log.info(customer_name + " : INSERT INTO " + unions_next_rank);
        	insertStatement.execute();        	
        }
        
        // adds unions of higher ranks for all nodes to structured_data_inc
        try (CallableStatement callStatement = con.prepareCall("{call AddUnions}"))
        {
        	log.info(customer_name + " :  {call AddUnions}");
           	callStatement.executeUpdate();
        }
        catch (CommunicationsException ex)
        {
        	// TODO: ignoring it for now until we figure out how to set timeout higher than 5 sec.
        	log.severe(customer_name + " : AddUnions thrown " + ex.getMessage());
        }
//        catch (Exception ex)
//        {
//        	log.severe(ex.getMessage());
//        	throw ex;
//        }
        log.info(customer_name + " : Inventory was loaded!");
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
			log.severe(customer_name + " : There are " + String.valueOf(inventorydata.getSegments().length) + " (more than allowed " + String.valueOf(BITMAP_SIZE) + ") inventory sets in " + readChannel.toString());
			wrongFile();
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
			
			BaseSet tmp = new BaseSet(BITMAP_SIZE);
			tmp.setkey(highBit);
			tmp.setname(is.getName());
			tmp.setCriteria(is.getCriteria());
			base_sets.put(tmp.getkey(), tmp);
			highBit++;
			log.info(customer_name + " added set " + tmp.getname() + " with criteria " + tmp.getCriteria().toString());
		}
		if (highBit == 0)
		{
			log.severe(customer_name + " : no data in inventory sets in " + readChannel.toString());
			wrongFile();
			return true;
		}			
		
		// Create segments' raw data. TODO: write into DB from the start
		HashMap<BitSet, BaseSegement> base_segments = new HashMap<BitSet, BaseSegement>();
		for (opportunity opp : inventorydata.getOpportunities())
		{
			boolean match_found = false;
			BaseSegement tmp = new BaseSegement(BITMAP_SIZE);
			tmp.setCriteria(opp.getcriteria());
			
			for (BaseSet bs1 : base_sets.values())
			{					
				if (bs1.getCriteria() == null && tmp.getCriteria() == null)
				{
					tmp.getkey().or(bs1.getkey());
					match_found = true;
				}
				else if (bs1.getCriteria() == null)
				{
					// because tmp.criteria isn't null no need to compare
					continue;
				}
				else if (criteria.matches(bs1.getCriteria(), tmp.getCriteria()))
				{
					tmp.getkey().or(bs1.getkey());
					match_found = true;
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
			log.severe(customer_name + " : no data in segments " + readChannel.toString());
			wrongFile();
			return true;
		}
		else if (base_segments.size() > INVENTORY_OVERLAP)
		{
			log.severe(customer_name + " : segments overlap is too high, " + Integer.toString(base_segments.size()) + " for file " + readChannel.toString());
			wrongFile();
			return true;
		}

		//
		// Populate all tables 
		//
        // populate structured data with inventory sets
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
        	log.info(customer_name + " : UPDATE " + structured_data_base);
        	
//        	// populate inventory sets table
//        	st.executeUpdate("INSERT INTO " 
//        			+ structured_data_inc
//        			+ " SELECT set_key, set_name, capacity, availability, goal FROM " 
//        			+ structured_data_base 
//        			+ " WHERE capacity IS NOT NULL");
//        	log.info(customer_name + " : INSERT INTO " + structured_data_inc);
        }
        
        // start union_next_rank table
        try (PreparedStatement insertStatement = con.prepareStatement("INSERT INTO " + unions_next_rank
//        		+ " SELECT * FROM structured_data_inc"))
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
    		}
			// adds unions of higher rank for nodes to of structured_data_inc
			try (CallableStatement callStatement = con.prepareCall("{call AddUnionsDynamic}")) {
				log.info(customer_name + " : iteration = " +  String.valueOf(ind) + " {call AddUnionsDynamic}");
				callStatement.executeUpdate();
			}
			catch (CommunicationsException ex)
			{
				log.severe(customer_name + " : loadDynamic call AddUnionsDynamic thrown " + ex.getMessage());
	    		// Reconnect back because the exception closed the connection.
				timeoutHandler.reconnect();
			}
			catch (java.sql.SQLException ex)
			{
				log.severe(customer_name + " : loadDynamic call AddUnionsDynamic thrown " + ex.getMessage());
				wrongFile();
				rs.close();
				return true;
			}
    		// AddUnionsDynamic completed	
			
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
				
				st.executeUpdate(
					"DELETE " + unions_last_rank + " FROM " + unions_last_rank
    			+ "        INNER JOIN " + unions_next_rank + " unr1"
    			+ "        WHERE (" + unions_last_rank + ".set_key & unr1.set_key) = " + unions_last_rank + ".set_key "
    			+ "        AND " + unions_last_rank + ".capacity = unr1.capacity");

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
    			+ "    SELECT * FROM " + unions_last_rank);
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
		try (Statement st = con.createStatement()) 
		{
			st.executeUpdate(" REPLACE INTO " + inventory_status + " VALUES(1, '" + Status.loaded.name() + "');");      			
		}

		log.info(customer_name + " : Inventory was loaded!");
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
    	long capacity = 0;
    	long availability = 0;
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
    
    
    @Override
    public void close() throws SQLException
    {
    	if(con!=null && !con.isClosed())
    	{
    		con.close();
    	}
    }
}
