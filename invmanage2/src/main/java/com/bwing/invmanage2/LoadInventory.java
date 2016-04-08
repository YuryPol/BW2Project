package com.bwing.invmanage2;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.util.logging.Logger;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import com.google.appengine.api.utils.SystemProperty;
import com.google.appengine.tools.cloudstorage.GcsFileOptions;
import com.google.appengine.tools.cloudstorage.GcsFilename;
import com.google.appengine.tools.cloudstorage.GcsInputChannel;
import com.google.appengine.tools.cloudstorage.GcsOutputChannel;
import com.google.appengine.tools.cloudstorage.GcsService;
import com.google.appengine.tools.cloudstorage.GcsServiceFactory;
import com.google.appengine.tools.cloudstorage.RetryParams;
import com.google.cloud.sql.jdbc.ResultSet;

public class LoadInventory extends HttpServlet 
{
	// Read file from the bucket and create tables in DB
   private static final Logger log = Logger.getLogger(FileServlet.class.getName());
   private String bucketName = "bw2project_data";
   /**Used below to determine the size of chucks to read in. Should be > 1kb and < 10MB */
   private static final int BUFFER_SIZE = 2 * 1024 * 1024;
   private final GcsService gcsService = GcsServiceFactory.createGcsService(new RetryParams.Builder()
		    .initialRetryDelayMillis(10)
		    .retryMaxAttempts(10)
		    .totalRetryPeriodMillis(15000)
		    .build());
   private static ObjectMapper mapper = new ObjectMapper();
   private static JsonFactory factory = mapper.getFactory();

    @Override
    public void doGet( HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException 
    {
    	String url;
        String customer_name = request.getParameter("customer_name");

		try 
		{
			// Connect to DB
			if (SystemProperty.environment.value() == SystemProperty.Environment.Value.Production) 
			{
				// Load the class that provides the new "jdbc:google:mysql://"
				// prefix.
//				Class.forName("com.mysql.jdbc.GoogleDriver");
				url = "jdbc:google:mysql://<your-project-id>:<your-instance-name>/<your-database-name>?user=root";
			} 
			else 
			{
				// Local MySQL instance to use during development.
//				Class.forName("com.mysql.jdbc.Driver"); // can't find the class
				url = "jdbc:mysql://localhost:3306/demo?user=root&password=IraAnna12";
			}
	        Connection con = null;
	        Statement st = null;
	        CallableStatement cs = null;
	        PreparedStatement insertStatement = null;
            con = DriverManager.getConnection(url);
			
			// Does the tables exist?
            java.sql.ResultSet rs = con.getMetaData().getTables(null, null, customer_name + "_raw_inventory_ex", null);
            if (!rs.next())
            {
				// Process the file
				GcsFilename gcsfileName = new GcsFilename(bucketName, customer_name);
            	if (gcsService.getMetadata(gcsfileName) == null)
            	{
            		// No file, request upload
                    response.sendRedirect("/"); // redirect to LoadInventory                     
                    return; // We don't want to upload multiple files for now
            	}
				GcsInputChannel readChannel = gcsService.openPrefetchingReadChannel(gcsfileName, 0, BUFFER_SIZE);
	
				//convert json input to object
				InventroryData inventorydata= mapper.readValue(Channels.newInputStream(readChannel), InventroryData.class);
	
	//	        InputStream in = null;
	//	        JsonParser parser = null;
	//			parser = factory.createParser(in);
	//			parser.nextToken();// JsonToken.START_OBJECT
	//			JsonToken token = null;
	//			while ((token = parser.nextToken()) == JsonToken.FIELD_NAME) {
	//				String name = parser.getText();
	//				parser.nextToken(); // JsonToken.START_OBJECT
	//				results.add(parser.readValueAs(classMap.get(name)));
	//			}
				
				// Create all tables
				
				// Populate all tables
            }			
		}
		catch (Exception ex) 
        {
            throw new ServletException(ex);
        }
    }
    
    
}
