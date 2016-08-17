package com.bwing.invmanage2;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.logging.Logger;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.fasterxml.jackson.core.JsonParseException;
import com.google.appengine.tools.cloudstorage.GcsFilename;
import com.google.appengine.tools.cloudstorage.GcsInputChannel;
import com.google.appengine.tools.cloudstorage.GcsService;
import com.google.appengine.tools.cloudstorage.GcsServiceFactory;
import com.google.appengine.tools.cloudstorage.RetryParams;
import com.mysql.jdbc.exceptions.jdbc4.CommunicationsException;


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
		
		log.info("Post loading " + file_name);
//		Connection con = null; 
		InventoryState invState;
		try {
			invState = new InventoryState(customer_name, true);
		} catch (ClassNotFoundException | SQLException ex) {
			// TODO Auto-generated catch block
			ex.printStackTrace();
			log.severe(customer_name + ex.toString());
			throw new ServletException(ex);
		}
		try {
//			con = invState.getConnection();
			// Process the file
			GcsFilename gcsfileName = new GcsFilename(InventoryFile.bucketName, file_name);
			if (gcsService.getMetadata(gcsfileName) == null) {
				// No file, request upload
				// invState.unlock();
				response.getWriter().println(
						"You are missing the file with " + customer_name + " inventory Data. Upload it or work with Test Inventory ");
				response.sendRedirect("/SelectInventory.jsp");
				log.warning(customer_name + " missing the file with Inventory Data");
				return;
			}
			GcsInputChannel readChannel = gcsService.openPrefetchingReadChannel(gcsfileName, 0, BUFFER_SIZE);

			invState.load(readChannel);
//			invState.unlock();
//			con.commit();
			log.info(file_name + " was parsed successfuly.");
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
			try 
			{
				invState.wrongFile();
			} 
			catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}			
/*			response.getWriter().println(
					"The file with " + customer_name + " inventory Data has wrong format. Upload new file or work with Test Inventory ");
			response.sendRedirect("/SelectInventory.jsp");
*/		
		}
		catch (CommunicationsException ex)
		{
			//TODO: this is temporary hack because it GAE throws the exception after 5 sec timeout
			log.severe(customer_name + ex.toString());			
			ex.printStackTrace();			
		}
		catch (com.mysql.jdbc.exceptions.jdbc4.MySQLNonTransientConnectionException ex)
		{
			//TODO: this is temporary hack because it GAE throws the exception after 5 sec timeout
			log.severe(customer_name + ex.toString());			
			ex.printStackTrace();			
		}
		catch (Exception ex) {
			ex.printStackTrace();
			log.severe(customer_name + ex.toString());
			throw new ServletException(ex);
		}
	}
		
}
