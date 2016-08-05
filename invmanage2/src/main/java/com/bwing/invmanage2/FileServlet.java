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

import org.apache.commons.fileupload.FileItemIterator;
import org.apache.commons.fileupload.FileItemStream;
import org.apache.commons.fileupload.servlet.ServletFileUpload;

import com.google.appengine.tools.cloudstorage.GcsFileOptions;
import com.google.appengine.tools.cloudstorage.GcsFilename;
import com.google.appengine.tools.cloudstorage.GcsInputChannel;
import com.google.appengine.tools.cloudstorage.GcsOutputChannel;
import com.google.appengine.tools.cloudstorage.GcsService;
import com.google.appengine.tools.cloudstorage.GcsServiceFactory;
import com.google.appengine.tools.cloudstorage.RetryParams;

@SuppressWarnings("serial")
public class FileServlet extends HttpServlet {

    private static final Logger log = Logger.getLogger(FileServlet.class.getName());

    private final GcsService gcsService = GcsServiceFactory.createGcsService(new RetryParams.Builder()
    .initialRetryDelayMillis(10)
    .retryMaxAttempts(10)
    .totalRetryPeriodMillis(15000)
    .build());

    /**Used below to determine the size of chucks to read in. Should be > 1kb and < 10MB */
      static final int BUFFER_SIZE = 2 * 1024 * 1024;
      private static final int MAX_FILE_LENGTH = 100 * 1024; // 100 KB limit for free version TODO: set higher for professional version.

    // @SuppressWarnings("unchecked")
    @Override
    public void doPost(HttpServletRequest req, HttpServletResponse response)
            throws ServletException, IOException {

        String sctype = null;
        ServletFileUpload upload;
        FileItemIterator iterator;
        FileItemStream item;
        InputStream stream = null;
        try 
        {
            upload = new ServletFileUpload();
            response.setContentType("text/plain");

            iterator = upload.getItemIterator(req);
            while (iterator.hasNext()) 
            {
                item = iterator.next();
                stream = item.openStream();

                String sfieldname = item.getFieldName();
 
                sctype = item.getContentType();

                if (item.isFormField()) 
                {
                    log.warning("Got a form field: " + item.getFieldName());
                } 
                else 
                {
                    log.warning("Got an uploaded file: " + item.getFieldName() +
                            ", name = " + item.getName());

                    sctype = item.getContentType();

                    GcsFilename gcsfileName = new GcsFilename(InventoryFile.bucketName, sfieldname);

                    GcsFileOptions options = new GcsFileOptions.Builder()
                    .acl("public-read").mimeType(sctype).build();

                    GcsOutputChannel outputChannel =
                            gcsService.createOrReplace(gcsfileName, options);

                    copy(stream, Channels.newOutputStream(outputChannel));

                    response.sendRedirect("/SelectInventory.jsp"); // redirect to SelectInventory 
                    
                    break; // We don't want to upload multiple files for now
                }
            }
        } catch (Exception ex) {
        	log.severe(ex.toString());
            throw new ServletException(ex);
        }
    }

    @Override
    public void doGet( HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException 
    {
        String customer_name = request.getParameter("customer_name");

        // You must tell the browser the file type you are going to send
        // for example application/pdf, text/plain, text/html, image/jpg
        response.setContentType("text/plain");

        // Make sure to show the download dialog
        response.setHeader("Content-disposition","attachment; filename=" + customer_name + ".json");

        try 
        {
        	// This should send the file to browser
            GcsFilename gcsfileName = new GcsFilename(InventoryFile.bucketName, customer_name);
            GcsInputChannel readChannel = gcsService.openPrefetchingReadChannel(gcsfileName, 0, BUFFER_SIZE);
            copy(Channels.newInputStream(readChannel), response.getOutputStream());
            
//            response.sendRedirect("/");
        } 
        catch (Exception ex) 
        {
        	log.severe(ex.toString());
            throw new ServletException(ex);
        }
   }

    private void copy(InputStream input, OutputStream output) throws Exception {
    	int length = 0;
        try {
          byte[] buffer = new byte[BUFFER_SIZE];
          int bytesRead = input.read(buffer);
          while (bytesRead != -1) {
          	if (length > MAX_FILE_LENGTH)
        	{
          		log.severe("Inventory file length exceeds " + Integer.toString(MAX_FILE_LENGTH) + " bytes");
        		throw new ServletException("Inventory file length exceeds " + Integer.toString(MAX_FILE_LENGTH) + " bytes");
        	}
            output.write(buffer, 0, bytesRead);
            bytesRead = input.read(buffer);
            length += bytesRead;
          }
        } finally {
          input.close();
          output.close();
        }
      }

}