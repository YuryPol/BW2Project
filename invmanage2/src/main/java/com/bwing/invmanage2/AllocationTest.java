package com.bwing.invmanage2;

import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;

import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Logger;

public class AllocationTest {

	public static void main(String[] args) throws ClassNotFoundException, SQLException {
	    Logger log = Logger.getLogger(AllocationTest.class.getName());
	    log.info("Testing allocations");
	    
		String customer_name = args[0];
        InventoryState invState = new InventoryState(customer_name);
        if (!invState.isLoaded())
        {
        	log.warning("The inventory " + customer_name + " is " + invState.getStatus().name() + ". Nothing to allocate");
        }
        else
        {
            Statement st = invState.getConnection().createStatement();
            st.execute("USE " + InventoryState.BWdb + customer_name);
            ResultSet rs;
            while (true)
            {
            	rs = st.executeQuery("SELECT count(*) FROM structured_data_base WHERE availability > 0");
            	rs.next();
            	int count = rs.getInt(1);
            	if (count == 0)
            	{
          			log.info("Completed sucessfuly");
          			invState.close();
          			return;            		
            	}
        		int current = ThreadLocalRandom.current().nextInt(1, count + 1);
            	rs = st.executeQuery("SELECT set_name, capacity, availability, goal FROM structured_data_base WHERE availability > 0");
            	if (!rs.next())
            	{
            		log.severe("something bad happen");
          			invState.close();
            		return;
            	}
        		// get one
            	if (current > 1)
            	{
            		rs.relative(current - 1);
            	}
        		String set_name = rs.getString("set_name");
        		int availability = rs.getInt("availability");
           		int alloc_Amount = ThreadLocalRandom.current().nextInt(1, availability + 1);

                log.info(set_name + " : " + Integer.toString(alloc_Amount));
           		invState.GetItems(set_name, alloc_Amount);
           		
           		rs = st.executeQuery("SELECT set_name FROM structured_data_base WHERE availability < 0");
           		if (rs.next())
           		{
           			log.severe("name, capacity, goal, availability");
               		do 
               		{
               			log.severe(rs.getString(1) + " , " + Integer.toString(rs.getInt(2)) + " , " 
               			+ Integer.toString(rs.getInt(3)) + " , " + Integer.toString(rs.getInt(4)));
               		} while (rs.next());
           		}
            }       	
        }
        invState.close();
	}
}
