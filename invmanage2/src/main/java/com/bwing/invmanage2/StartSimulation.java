package com.bwing.invmanage2;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.appengine.api.taskqueue.Queue;
import com.google.appengine.api.taskqueue.QueueFactory;
import com.google.appengine.api.taskqueue.TaskOptions;

@SuppressWarnings("serial")
public class StartSimulation extends HttpServlet {
	// Read file from the bucket and create tables in DB
	private static final Logger log = Logger.getLogger(StartSimulation.class.getName());

	@Override
	public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// Start simulation task
		String customer_name = request.getParameter("customer_name");
		try (InventoryState invState = new InventoryState(customer_name, true)) {
//			invState.lock();
			Connection con = invState.getConnection();

			DatabaseMetaData dbm = con.getMetaData();
			// check if "result_serving" table is there
			ResultSet tables = dbm.getTables(null, null, "result_serving", null);
			if (tables.next()) {
				log.warning("Table " + customer_name + ".result_serving already exists, somebody else is running the simulation");
				response.sendRedirect("/WaitSimulation.jsp?message=wasRunning");
			} else {
				// Table does not exist, go ahead
				// Create the table
	            Statement st = con.createStatement();
	            st.executeUpdate("CREATE TABLE result_serving  ENGINE=MEMORY AS SELECT *, 0 AS served_count FROM structured_data_base;");
				// And queue the task
		    	Queue queue = QueueFactory.getDefaultQueue();
		    	queue.add(TaskOptions.Builder.withUrl("/simulate").param("customer_name", customer_name));
		    	log.info(customer_name + " simulation added to default queue.");
				// go to waiting page
		    	TimeUnit.SECONDS.sleep(1);
				response.sendRedirect("/WaitSimulation.jsp");
			}
		} catch (ClassNotFoundException | SQLException ex) {
			log.severe(customer_name + "error " + ex.toString());
            throw new ServletException(ex);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
