package com.bwing.invmanage2;
import java.io.IOException;
import java.util.logging.Logger;

import com.google.appengine.tools.cloudstorage.GcsFilename;
import com.google.appengine.tools.cloudstorage.GcsService;
import com.google.appengine.tools.cloudstorage.GcsServiceFactory;
import com.google.appengine.tools.cloudstorage.RetryParams;

public class InventoryFile 
{
	public InventoryFile(String customer_name)
	{
        gcsfileName = new GcsFilename(bucketName, customer_name);
	}
	
	private GcsFilename gcsfileName;
	
    private static final Logger log = Logger.getLogger(InventoryFile.class.getName());

    private final GcsService gcsService = GcsServiceFactory.createGcsService(new RetryParams.Builder()
    .initialRetryDelayMillis(10)
    .retryMinAttempts(1)
    .retryMaxAttempts(3)
    .totalRetryPeriodMillis(5000)
    .build());

    public final static String bucketName = "bw2project_data";
    
    public boolean isLoaded()
    {
    	try {
			return gcsService.getMetadata(gcsfileName) != null;
		} catch (IOException e) {
			log.warning("Can't open file " + gcsfileName.getBucketName() + "." + gcsfileName.getObjectName());
			return false;
		}
    }

}
