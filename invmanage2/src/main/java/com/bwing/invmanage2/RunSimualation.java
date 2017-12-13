package com.bwing.invmanage2;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.appengine.api.taskqueue.Queue;
import com.google.appengine.api.taskqueue.QueueFactory;
import com.google.appengine.api.taskqueue.TaskOptions;
import com.google.appengine.api.utils.SystemProperty;

public class RunSimualation {
	private static final Logger log = Logger.getLogger(RunSimualation.class.getName());
	private static final long RESTART_INTERVAL = 600000 - 10000; // less than 10 minutes for running in local app-engine simulator

	public static boolean runIt(InventoryState invState, boolean test) throws SQLException
	{
		// Run simulation
		Calendar starting = new GregorianCalendar();
		Long start = starting.getTimeInMillis();
		Long interval;
		String customer_name = invState.customer_name;
		log.setLevel(Level.INFO);
		Connection con = invState.getConnection();
        ResultSet rs = null;
        int increment = 1; // TODO: consider setting it higher for faster runs.
        Random rand= new Random();
        long served_count = 0;
        long missed_count = 0;
        int rand_weight = 0;

        Statement st = con.createStatement();
        st.executeUpdate("DROP TABLE IF EXISTS " + InventoryState.result_serving_copy);
        st.executeUpdate("DROP TABLE IF EXISTS " + InventoryState.result_serving);
        st.executeUpdate("CREATE TABLE " + InventoryState.result_serving + " ENGINE=MEMORY AS SELECT *, 0 AS served_count FROM " + InventoryState.structured_data_base);
        // process result_serving table
        PreparedStatement max_weightStatement = con.prepareStatement("select max(weight) from " + InventoryState.raw_inventory);
        rs = max_weightStatement.executeQuery();
        if (!rs.next())
        {
        	log.warning(customer_name + " : no raw data ");
        	return true; // no raw data
        }
        int max_weight = Math.abs(rs.getInt(1));
        PreparedStatement somethingLeft = con.prepareStatement("select max(goal - served_count) as maxRemained from " + InventoryState.result_serving);
        PreparedStatement getRequest = con.prepareStatement("select basesets from " + InventoryState.raw_inventory + " where weight >= ? order by weight asc limit 1");
        PreparedStatement choseInventorySet = con.prepareStatement(
        		"select set_key_is, (goal-served_count)/goal as weight_now from " + InventoryState.result_serving
        		+ " where goal > served_count and ( ? & set_key_is ) = set_key_is order by weight_now desc limit 1;");
        PreparedStatement incrementServedCount = con.prepareStatement("update " + InventoryState.result_serving + " set served_count = served_count + "
        		+ Integer.toString(increment) + " where set_key_is = ?");
        //
        // Process raw inventory
        //
        for(int i = 0; i < max_weight; i += increment) {
        	// Are some unfulfilled goals left?
            rs = somethingLeft.executeQuery();
            if (!rs.next())
            	break; // shouldn't happen
            if (rs.getInt("maxRemained") <= 0)
            	break; // all goals are fulfilled.            	
        	
            // create the request
        	rand_weight = rand.nextInt(max_weight);
            getRequest.setInt(1, rand_weight);
            rs = getRequest.executeQuery();
            if (!rs.next()) // should not happen
            {
            	log.severe(customer_name + " : no datg for weight = " + String.valueOf(rand_weight));
            	continue;
            }
            long basesets = rs.getLong(1);
            // select inventory set to serve
            choseInventorySet.setLong(1, basesets);
            rs = choseInventorySet.executeQuery();
            long set_key_is = 0;
            if (rs.next())
            {
            	// not all matching inventory sets were served up
            	// otherwise record the miss
            	set_key_is = rs.getLong(1);
            	incrementServedCount.setLong(1, set_key_is);
            	incrementServedCount.executeUpdate();
            	served_count += increment;
            }
            else
            	missed_count++;
            // increment served_count in result_serving
            if (served_count != 0 && served_count % 10000 == 0)
            {
            	log.info(customer_name + " : " + new SimpleDateFormat("MM/dd/yyyy HH:mm:ss").format(new Date())
            			+ " : served_count=" +  String.valueOf(served_count) + ", missed_count=" 
                        + String.valueOf(missed_count) + "\r");
            }
            Calendar current = new GregorianCalendar();
            interval = current.getTimeInMillis() - start;
            if (!(SystemProperty.environment.value() == SystemProperty.Environment.Value.Production) && 
            		interval >= RESTART_INTERVAL && !test)
            {
    	    	Queue queue = QueueFactory.getDefaultQueue();
    	    	queue.add(TaskOptions.Builder.withUrl("/simulate").param("customer_name", customer_name));
            	log.warning(customer_name + " : Restarting the simuilation after " + interval.toString() + " msec.");
            	return true;
            }	            	
        }
        st.executeUpdate("CREATE TABLE " + InventoryState.result_serving_copy + " ENGINE=MEMORY AS SELECT * FROM " + InventoryState.result_serving);
        st.executeUpdate("DROP TABLE IF EXISTS " + InventoryState.result_serving);
        log.info(customer_name + " : " + new SimpleDateFormat("MM/dd/yyyy HH:mm:ss").format(new Date())
        		+ " : served_count=" +  String.valueOf(served_count) + ", missed_count=" + String.valueOf(missed_count));
        
        PreparedStatement totalInventoryStatement = con.prepareStatement("select max(weight) from " + InventoryState.raw_inventory);
        rs = totalInventoryStatement.executeQuery();
        if (!rs.next())
        {
        	log.warning(customer_name + " : no raw data ");
        	return true; // no raw data
        }
        int totalInventory = Math.abs(rs.getInt(1));
        log.info(customer_name + " : out of total inventory " + String.valueOf(totalInventory));
        if (missed_count > served_count / 100 && served_count > 100)
        {
        	log.severe(customer_name + " : too many mssies");
        	return false;
        }        	
        else
        {
        	log.info(customer_name + " : simulation compelted successfuly");
        	return true;
        }
	} 
}


