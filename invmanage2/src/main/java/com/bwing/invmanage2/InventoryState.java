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
import java.text.SimpleDateFormat;
import java.util.BitSet;
import java.util.Date;
import java.util.HashMap;
import java.util.Properties;
import java.util.logging.Logger;

import org.mortbay.log.Log;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.appengine.api.utils.SystemProperty;
import com.google.appengine.tools.cloudstorage.GcsInputChannel;
import com.mysql.jdbc.exceptions.jdbc4.CommunicationsException;

import java.sql.ResultSet;
;

public class InventoryState implements AutoCloseable
{
	private static final Logger log = Logger.getLogger(InventoryState.class.getName());

	public enum Status {
		clean, invalid, loaded, wrongfile
	}

	String customer_name;
    Connection con;
    private static ObjectMapper mapper = new ObjectMapper();
    private static final Logger Log = Logger.getLogger(InventoryState.class.getName());
    
    // DB prefix
    public static final String BWdb = "BWing_";
    
    // Tables
    static final String raw_inventory_ex = "raw_inventory_ex";
    public static final String raw_inventory = "raw_inventory";
    static final String structured_data_inc ="structured_data_inc";
    static final String structured_data_base = "structured_data_base";
    static final String unions_last_rank = "unions_last_rank";
    static final String unions_next_rank = "unions_next_rank";
    public static final String allocation_ledger = "allocation_protocol";
    static final String inventory_status = "inventory_status";
 
	static final int BITMAP_SIZE = 64;
	
	public static Connection connect(boolean auto) throws ClassNotFoundException, SQLException
	{
//		Log.info(Status.loaded.name());
		
		String url;
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
		Connection con = DriverManager.getConnection(url, info);
		con.setAutoCommit(auto);
		return con;
	}
    
    public InventoryState(String name, boolean auto) throws ClassNotFoundException, SQLException
	{
    	con = connect(auto);
    	customer_name = name;
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
        	+ " status VARCHAR(200) DEFAULT '" + Status.invalid.name()
        	+ "', PRIMARY KEY(fake_key))");

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
        	
        	st.executeUpdate("DROP TABLE IF EXISTS " + allocation_ledger);
        	st.executeUpdate("CREATE TABLE " + allocation_ledger
            	    + " (set_key BIGINT DEFAULT NULL, "
            	    + "set_name VARCHAR(20) DEFAULT NULL, "
            	    + "capacity INT DEFAULT NULL, " 
            	    + "availability INT DEFAULT NULL, " 
            	    + "advertiserID  VARCHAR(80) DEFAULT NULL, "
            	    + "goal INT DEFAULT 0, "
            	    + "PRIMARY KEY(advertiserID))");
        	
        	st.executeUpdate("DROP PROCEDURE IF EXISTS PopulateRankWithNumbers");
        	st.executeUpdate("CREATE PROCEDURE PopulateRankWithNumbers() "
        			+ "BEGIN "
        			+ " UPDATE unions_next_rank, "
        			+ " (SELECT "
        			+ "    set_key, "
        			+ "    SUM(capacity) as capacity, "
        			+ "    SUM(availability) as availability "
        			+ "  FROM ("
        			+ "   SELECT unr1.set_key, ri.count as capacity, ri.count as availability "
        			+ "    FROM unions_next_rank unr1"
        			+ "    JOIN raw_inventory ri "
        			+ "    ON unr1.set_key & ri.basesets != 0 "
        			+ "    WHERE unr1.capacity is NULL "
        			+ "    ) blownUp "
        			+ "  GROUP BY set_key"
        			+ " ) comp "
        			+ " SET unions_next_rank.capacity = comp.capacity,"
        			+ "     unions_next_rank.availability = comp.availability "
        			+ " WHERE unions_next_rank.set_key = comp.set_key; "
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
        			+ "     UPDATE structured_data_base sdbR, structured_data_inc sd "
        			+ "     SET sdbR.availability = LEAST(sdbR.availability, sd.availability) "
        			+ "     WHERE sd.set_key & sdbR.set_key_is = sdbR.set_key_is; "
        			+ "     SELECT 'passed'; "
        			+ "   ELSE "
        			+ "     SELECT 'failed'; "
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
        	st.executeUpdate("TRUNCATE " + raw_inventory_ex);
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
        	Log.severe("connection problem: " + ex.getMessage());
        	return false;
        }
    }

    public Status getStatus() throws SQLException // TODO: do we really need it?
    {
        try (Statement st = con.createStatement())
        {
        	ResultSet rs = st.executeQuery("SELECT * FROM " + inventory_status);
        	if (rs.next())
        	{
        		log.info("Status is " + rs.getString(2));
        		return Status.valueOf(rs.getString(2));
        	}
        	else
        	{
        		return Status.invalid;
        	}
        }
    }
    
    void lock() throws SQLException // TODO: remove later
    {
        try (Statement st = con.createStatement())
        {
        	boolean res = st.execute("LOCK TABLES "+ raw_inventory_ex + " WRITE, "
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
        	Log.warning(raw_inventory_ex + " was locked");
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
        	Log.warning("databases unlocked");
       }
    }
    
    public void invalidate() throws SQLException
    {
        try (Statement st = con.createStatement())
        {
        	clear();
        	st.executeUpdate("REPLACE INTO " + inventory_status + " VALUES(1, '" + Status.invalid.name() + "')");
        	Log.info("status invalidated");
        }
    }
    
    public void wrongFile() throws SQLException
    {
        try (Statement st = con.createStatement())
        {
        	clear();
        	st.executeUpdate("REPLACE INTO " + inventory_status + " VALUES(1, '" + Status.wrongfile.name() + "')");
        	Log.info("status set to wrong file");
        }
    }
    public void validate()
    {
        try (Statement st = con.createStatement())
        {
        	st.executeUpdate("REPLACE INTO " + inventory_status + " VALUES(1, '" + Status.loaded.name() + "')");
        	Log.info("status validated");
        } catch (SQLException ex) {
			// TODO Auto-generated catch block
			ex.printStackTrace();
			log.severe(customer_name + ex.toString());
		}
    }
    
    public boolean isLoaded() throws SQLException
    {
    	if (getStatus() == Status.loaded)
    		return true;
    	else
    		return false;
    }
    
    public boolean isValid() throws SQLException
    {
    	if (getStatus() == Status.invalid)
    		return false;
    	else
    		return true;
    }
           
    public boolean isWrongFile() throws SQLException
    {
    	if (getStatus() == Status.wrongfile)
    		return true;
    	else
    		return false;
    }

    public void load(ReadableByteChannel readChannel) throws JsonParseException, JsonMappingException, IOException, SQLException
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
        try (CallableStatement callStatement = con.prepareCall("{call AddUnions}"))
        {
        	Log.info("{call AddUnions}");
           	callStatement.executeUpdate();
        }
        catch (CommunicationsException ex)
        {
        	// TODO: ignoring it for now until we figure out how to set timeout higher than 5 sec.
        	Log.severe("AddUnions thrown " + ex.getMessage());
        }
        catch (Exception ex)
        {
        	Log.severe(ex.getMessage());
        	throw ex;
        }
        Log.info("Inventory for " + customer_name + " was loaded!");
    }
    
    public void GetItems(String set_name, String advertiserID, int amount) throws SQLException
    {
    	if (amount <= 0)
    	{
			Log.warning("wrong allocation = " + amount);
			return;
    	}
    	if (advertiserID.length() == 0)
    	{
    		// replace advertiser ID with time-stamp
    		advertiserID = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss").format(new Date());
    	}
    	
    	long set_key_is = 0;
    	long capacity = 0;
    	long availability = 0;
    	String query = "SELECT set_key_is, capacity, availability FROM structured_data_base WHERE set_name = '" + set_name + "'";
    	try (Statement statement = con.createStatement())
    	{
    		ResultSet rs = statement.executeQuery(query);
	        if (rs.next())
	        {
	    		set_key_is = rs.getLong(1);
	    		capacity = rs.getInt(2);
	    		availability = rs.getInt(3);
	        }
	        else
	        	log.severe(set_name + " set wasn't found");
    	}
    	
    	try (CallableStatement callStatement = con.prepareCall("{call " + BWdb + customer_name + ".GetItemsFromSD(?, ?)}"))
    	{
    		callStatement.setLong(1, set_key_is);
    		callStatement.setInt(2, amount);
    		callStatement.execute();
    	}    			
    	
    	int result = 0;
    	try (PreparedStatement statement = con.prepareStatement(
    	"INSERT INTO " + allocation_ledger + " (set_key, set_name, capacity, availability, advertiserID, goal) VALUES ('"
    	+ String.valueOf(set_key_is) + "','" 
    	+ set_name + "','"
    	+ String.valueOf(capacity) + "','" 
    	+ String.valueOf(availability) + "','" 
    	+ advertiserID + "','" 
    	+ String.valueOf(amount) 
    	+ "') ON DUPLICATE KEY UPDATE goal = VALUES(goal) + goal" ))
    	{
    		result = statement.executeUpdate();
    	}
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
