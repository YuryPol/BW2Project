package com.bwing.invmanage2;

import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.appengine.api.taskqueue.Queue;
import com.google.appengine.api.taskqueue.QueueFactory;
import com.google.appengine.api.taskqueue.TaskOptions;

@SuppressWarnings("serial")
public class LoadInventory extends HttpServlet 
{
	// Read file from the bucket and create tables in DB
   private static final Logger log = Logger.getLogger(LoadInventory.class.getName());

    @Override
    public void doGet( HttpServletRequest request, HttpServletResponse response) throws ServletException 
    {
        String customer_name = request.getParameter("customer_name");
        String file_name = request.getParameter("file_name");
		log.setLevel(Level.INFO);

		try (InventoryState invState = new InventoryState(customer_name, true)) {
			if (invState.isLoadStarted() || invState.isLoadInProgress())
			{
		    	log.warning(customer_name + " inventory is loading");
				// go back
				response.sendRedirect("/WatitForInventory.jsp");
				return;
			}
			else
			{
				// Process the file
				invState.loadstarted();
		    	Queue queue = QueueFactory.getDefaultQueue();
		    	queue.add(TaskOptions.Builder.withUrl("/loadwork").param("file", file_name).param("customer_name", customer_name)
		    			//.header("Host", ModulesServiceFactory.getModulesService().getVersionHostname(null, null)));
		    			);
		    	log.info(file_name + " processing added to default queue.");
				// go to allocation page
				response.sendRedirect("/WatitForInventory.jsp");
				return;
			}
		}
		catch (Exception ex) 
        {
			ex.printStackTrace();
			log.severe(customer_name + "error " + ex.toString());
            throw new ServletException(ex);
        }
    }
    
    
}
