package com.bwing.invmanage2;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Random;
import java.util.logging.Logger;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@SuppressWarnings("serial")
public class Simulation extends HttpServlet {
	private static final Logger log = Logger.getLogger(Simulation.class.getName());

	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException
	{
		// Run simulation
		String customer_name = request.getParameter("customer_name");
		try (InventoryState invState = new InventoryState(customer_name)) {
//			invState.lock();
			Connection con = invState.getConnection();
	        ResultSet rs = null;
	        int increment = 1; // TODO: consider setting it for faster runs.
	        Random rand= new Random();
	        int served_count = 0;
	        int missed_count = 0;
	        int rand_weight = 0;

            Statement st = con.createStatement();
            st.executeUpdate("DROP TABLE IF EXISTS result_serving");
            st.executeUpdate("CREATE TABLE result_serving  ENGINE=MEMORY AS SELECT *, 0 AS served_count FROM structured_data_base;");
            st.executeUpdate("DROP TABLE IF EXISTS result_serving_copy");
            // process result_serving table
            PreparedStatement max_weightStatement = con.prepareStatement("select max(weight) from raw_inventory");
            rs = max_weightStatement.executeQuery();
            if (!rs.next())
            	return; // no raw data
            int max_weight = Math.abs(rs.getInt(1));
            PreparedStatement somethingLeft = con.prepareStatement("select max(goal - served_count) as maxRemained from result_serving");
            PreparedStatement getRequest = con.prepareStatement("select basesets from raw_inventory where weight >= ? order by weight asc limit 1");
            PreparedStatement choseInventorySet = con.prepareStatement(
            		"select set_key_is, (goal-served_count)/goal as weight_now from result_serving "
            		+ "where goal > served_count and ( ? & set_key_is ) = set_key_is order by weight_now desc limit 1;");
            PreparedStatement incrementServedCount = con.prepareStatement("update result_serving set served_count = served_count + "
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
	            	log.severe("no datga for weight = " + String.valueOf(rand_weight));
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
	            	log.info(new SimpleDateFormat("MM/dd/yyyy HH:mm:ss").format(new Date())
	            			+ " : served_count=" +  String.valueOf(served_count) + ", missed_count=" 
	                        + String.valueOf(missed_count) + "\r");
	            }
            }
            st.executeUpdate("CREATE TABLE result_serving_copy ENGINE=MEMORY AS SELECT * FROM result_serving");
            st.executeUpdate("DROP TABLE IF EXISTS result_serving");
            log.info(new SimpleDateFormat("MM/dd/yyyy HH:mm:ss").format(new Date())
            		+ " : served_count=" +  String.valueOf(served_count) + ", missed_count=" + String.valueOf(missed_count));            	            
		} catch (ClassNotFoundException | SQLException ex) {
			log.severe(ex.getMessage());
			throw new ServletException(ex);
		}
	}
}
