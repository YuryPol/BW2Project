package com.bwing.invmanage2;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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
		try (InventoryState invState = new InventoryState(customer_name)) {
			Connection con = invState.getConnection();

			DatabaseMetaData dbm = con.getMetaData();
			// check if "result_serving" table is there
	            createResultServingTable.executeUpdate();			
		    	Queue queue = QueueFactory.getDefaultQueue();
		    	queue.add(TaskOptions.Builder.withUrl("/simulate").param("customer_name", customer_name));
		    	log.info(customer_name + " simulation added to default queue.");
			}
		} catch (ClassNotFoundException | SQLException ex) {
			log.severe(customer_name + "error " + ex.toString());
            throw new ServletException(ex);
		}
	}
}
