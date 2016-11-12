package com.bwing.invmanage2;

import java.io.IOException;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.fasterxml.jackson.core.JsonParseException;
import com.google.appengine.api.taskqueue.Queue;
import com.google.appengine.api.taskqueue.QueueFactory;
import com.google.appengine.api.taskqueue.TaskOptions;
import com.google.appengine.api.utils.SystemProperty;
import com.google.appengine.tools.cloudstorage.GcsFilename;
import com.google.appengine.tools.cloudstorage.GcsInputChannel;
import com.google.appengine.tools.cloudstorage.GcsService;
import com.google.appengine.tools.cloudstorage.GcsServiceFactory;
import com.google.appengine.tools.cloudstorage.RetryParams;
import com.google.apphosting.api.DeadlineExceededException;

@SuppressWarnings("serial")
public class Loadworker  extends HttpServlet
{
	private static final Logger log = Logger.getLogger(Loadworker.class.getName());
	public static final int BUFFER_SIZE = 2 * 1024 * 1024;
	private final GcsService gcsService = GcsServiceFactory.createGcsService(new RetryParams.Builder()
			    .initialRetryDelayMillis(10)
			    .retryMaxAttempts(10)
			    .totalRetryPeriodMillis(15000)
			    .build());

	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException
	{
		String file_name = request.getParameter("file");
		String customer_name = request.getParameter("customer_name");
		log.setLevel(Level.INFO);
		
//		Connection con = null;
		GcsInputChannel readChannel = null;

		try (InventoryState invState = new InventoryState(customer_name, true)) 
		{
			if (/*invState.isClean()*/ file_name != null)
			{
				// Process the file
				log.info("Loading " + file_name);
				GcsFilename gcsfileName = new GcsFilename(InventoryFile.bucketName, file_name);
				if (gcsService.getMetadata(gcsfileName) == null) {
					// No file, request upload
					log.severe(customer_name + " missing the file with Inventory Data");
					return;
				}
				readChannel = gcsService.openPrefetchingReadChannel(gcsfileName, 0, BUFFER_SIZE);
			}

			if (!invState.loadDynamic(readChannel)
					&& !(SystemProperty.environment.value() == SystemProperty.Environment.Value.Production))
			{
				Queue queue = QueueFactory.getDefaultQueue();
				queue.add(TaskOptions.Builder.withUrl("/loadwork").param("customer_name", customer_name));
			}
//			con.commit();
		} catch (ClassNotFoundException ex) {
			// TODO Auto-generated catch block
			ex.printStackTrace();
			log.severe(customer_name + ex.toString());
			throw new ServletException(ex);
		}
		catch (JsonParseException ex) 
		{
//			try 
//			{
//				if (con != null)
//					con.rollback();
//			} 
			log.severe(customer_name + ex.toString());			
			ex.printStackTrace();
			try (InventoryState invState = new InventoryState(customer_name, true)) {
				invState.wrongFile();
			} 
			catch (SQLException | ClassNotFoundException ex1) {
				// TODO Auto-generated catch block
				ex1.printStackTrace();
			}			
		}
//		catch (CommunicationsException ex) Should be handled in load()
//		{
//			//TODO: this is temporary hack because it GAE throws the exception after 5 sec timeout
//			log.severe(customer_name + ex.toString());			
//			ex.printStackTrace();			
//		}
//		catch (com.mysql.jdbc.exceptions.jdbc4.MySQLNonTransientConnectionException ex)
//		{
//			//TODO: this is temporary hack because it GAE throws the exception after 5 sec timeout
//			log.severe(customer_name + ex.toString());			
//			ex.printStackTrace();			
//		}
		catch (com.mysql.jdbc.exceptions.jdbc4.MySQLSyntaxErrorException ex)
		{
			// May happen when user cancels load
			log.severe("Excepton " + ex.getMessage() + ". May happen when user cancels load");
			ex.printStackTrace();			
		}
        catch (DeadlineExceededException ex)
        {
        	// Caused by DeadlineExceededException
        	log.severe("Too much data. Caused DeadlineExceededException " + ex.getMessage());
			ex.printStackTrace();
			// And queue the task to continue after it timed out
//	    	Queue queue = QueueFactory.getDefaultQueue();
//	    	queue.add(TaskOptions.Builder.withUrl("/loadwork")
//	    			.param("customer_name", customer_name).param("file_name", ""));
			try (InventoryState invState = new InventoryState(customer_name, true)) {
				invState.toomuchdata();
			} catch (ClassNotFoundException | SQLException ex1) {
				// TODO Auto-generated catch block
				ex1.printStackTrace();
			} 
        }
		catch (SQLException ex) {
			ex.printStackTrace();
			log.severe(customer_name + ex.toString());
			throw new ServletException(ex);
		} catch (InterruptedException ex) {
			ex.printStackTrace();
			log.severe(customer_name + ex.toString());
			throw new ServletException(ex);
		}
	}
}
