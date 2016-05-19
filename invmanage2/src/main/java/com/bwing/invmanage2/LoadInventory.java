package com.bwing.invmanage2;

import java.io.IOException;
import java.util.logging.Logger;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.appengine.tools.cloudstorage.GcsFilename;
import com.google.appengine.tools.cloudstorage.GcsInputChannel;
import com.google.appengine.tools.cloudstorage.GcsService;
import com.google.appengine.tools.cloudstorage.GcsServiceFactory;
import com.google.appengine.tools.cloudstorage.RetryParams;

@SuppressWarnings("serial")
public class LoadInventory extends HttpServlet 
{
	// Read file from the bucket and create tables in DB
   private static final Logger log = Logger.getLogger(LoadInventory.class.getName());
   private String bucketName = "bw2project_data";
   /**Used below to determine the size of chucks to read in. Should be > 1kb and < 10MB */
   private static final int BUFFER_SIZE = 2 * 1024 * 1024;
   private final GcsService gcsService = GcsServiceFactory.createGcsService(new RetryParams.Builder()
		    .initialRetryDelayMillis(10)
		    .retryMaxAttempts(10)
		    .totalRetryPeriodMillis(15000)
		    .build());

    @Override
    public void doGet( HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException 
    {
        String customer_name = request.getParameter("customer_name");
        String file_name = request.getParameter("file_name");

		try (InventoryState invState = new InventoryState(customer_name)) {
			// Process the file
			GcsFilename gcsfileName = new GcsFilename(bucketName, file_name);
			if (gcsService.getMetadata(gcsfileName) == null) {
				// No file, request upload
				response.getWriter().println(
						"You are missing the file with your Inventory Data. Upload it or work with Test Inventory");
				response.sendRedirect("/SelectInventory.jsp");
				log.warning(customer_name + " missing the file with Inventory Data");
				return;
			}
			GcsInputChannel readChannel = gcsService.openPrefetchingReadChannel(gcsfileName, 0, BUFFER_SIZE);

			invState.clear();
			invState.load(readChannel);
			// go to allocation page
			response.sendRedirect("/Allocate.jsp?customer=" + customer_name);
		}
		catch (Exception ex) 
        {
            throw new ServletException(ex);
        }
    }
    
    
}
