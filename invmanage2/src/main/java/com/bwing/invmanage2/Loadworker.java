package com.bwing.invmanage2;

import java.io.IOException;
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


@SuppressWarnings("serial")
public class Loadworker  extends HttpServlet
{
	private static final Logger log = Logger.getLogger(Loadworker.class.getName());
	private static final int BUFFER_SIZE = 2 * 1024 * 1024;
	private final GcsService gcsService = GcsServiceFactory.createGcsService(new RetryParams.Builder()
			    .initialRetryDelayMillis(10)
			    .retryMaxAttempts(10)
			    .totalRetryPeriodMillis(15000)
			    .build());
	
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException
	{
		String file_name = request.getParameter("file");
		String customer_name = request.getParameter("customer_name");
		boolean isOK = true;
		
		log.info("Post loading " + file_name);
		try (InventoryState invState = new InventoryState(customer_name)) {
			invState.lock();
			invState.clear();
			// Process the file
			GcsFilename gcsfileName = new GcsFilename(InventoryFile.bucketName, file_name);
			if (gcsService.getMetadata(gcsfileName) == null) {
				// No file, request upload
				response.getWriter().println(
						"You are missing the file with your Inventory Data. Upload it or work with Test Inventory");
				response.sendRedirect("/SelectInventory.jsp");
				log.warning(customer_name + " missing the file with Inventory Data");
				return;
			}
			GcsInputChannel readChannel = gcsService.openPrefetchingReadChannel(gcsfileName, 0, BUFFER_SIZE);

			invState.load(readChannel);
		} catch (JsonParseException ex) {
			log.severe(customer_name + ex.toString());			
			ex.printStackTrace();
			isOK = false;
		} catch (SQLException ex) {
			// TODO: that persistent SQLException must be fixed
			ex.printStackTrace();
			log.severe(customer_name + ex.toString());
			throw new ServletException(ex);
			//isOK = true; // TODO: until persistent SQLException is fixed
		}
		catch (Exception ex) {
			ex.printStackTrace();
			log.severe(customer_name + ex.toString());
			throw new ServletException(ex);
			// isOK = false;
		}
		if (!isOK)
		{
			try (InventoryState invState = new InventoryState(customer_name)) {
				invState.invalidate();				
			}
			catch (Exception ex) {
				ex.printStackTrace();
				log.severe(customer_name + ex.toString());
				throw new ServletException(ex);
			}

		}
	}
		
}
