<%@ page import="com.bwing.invmanage2.InventoryUser" %>
<%@ page import="com.bwing.invmanage2.InventoryState" %>
<%@ page import="com.bwing.invmanage2.InventoryFile" %>
<%@ page import="com.bwing.invmanage2.Customer" %>
<%@ page import="com.googlecode.objectify.ObjectifyService" %>
<%@ page import="com.googlecode.objectify.Key" %>
<%@ page import="com.google.appengine.api.blobstore.BlobstoreServiceFactory" %>
<%@ page import="com.google.appengine.api.blobstore.BlobstoreService" %>
<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<%@ taglib prefix="fn" uri="http://java.sun.com/jsp/jstl/functions" %>

<html>
<head>
    <link type="text/css" rel="stylesheet" href="/stylesheets/main.css"/>
</head>

<body>
<%
    // check the user
    InventoryUser iuser = InventoryUser.getCurrentUser();
    if (iuser == null)
    {
        %>
        <p>ERROR! You were logged out</p>
        <p>Return to login page</p>
        <form action="/" method="get">
        <div><input type="submit" value="Return"/></div>
        </form>
        <%
    }
    else 
    {
        // registered user, let her chose/create inventory
        String customer_name = iuser.theCustomer.get().company;
        pageContext.setAttribute("customer_name", customer_name);
        %>
        <p>You can return to starting page,</p>
        <form action="/" method="get">
        <div><input type="submit" value="Return"/></div>
        </form>
        
        <% 
        InventoryState invState = new InventoryState(customer_name);
        {
        	if (invState.isLoaded())
        	{
        		%>
		        <p>Allocate items in your inventory,</p>
		        <form action="/Allocate.jsp" method="get">
		        <div><input type="submit" value="Allocate"/></div>
		        </form>
		        <p>Or you can start over and re-initialize your inventory</p>
        		<%
        	}
        }
        InventoryFile testFile = new InventoryFile("TestInventory");
        if (testFile.isLoaded())
        {
        %>        
	        <p>Initialize inventory with test data</p>
		    <form action="/load" method="get">
		        <div><input type="submit" value="Test Inventory"/></div>
	            <input type="hidden" name="file_name" value="TestInventory"/>
                <input type="hidden" name="customer_name" value="${fn:escapeXml(customer_name)}"/>
	        </form>        
        <%
        }
        else
        {
        %>
        	<p>ERROR: Test inventory file was not found</p>
        <%
        }
        InventoryFile invFile = new InventoryFile(customer_name);
        if (invFile.isLoaded()) 
        {        
        %>
	        <p>Initialize inventory with data you uploaded</p>
	        <form action="/load" method="get">
		        <div><input type="submit" value="Your Inventory"/></div>
                <input type="hidden" name="file_name" value="${fn:escapeXml(customer_name)}"/>
		        <input type="hidden" name="customer_name" value="${fn:escapeXml(customer_name)}"/>
	        </form>
            <p>or upload new data</p>
	    <% 
	    }
	    %>        
        <p>Upload a new inventory data file</p>
	    <form action="/gcs" method="post" enctype="multipart/form-data">
 	        <input type="file" name="${fn:escapeXml(customer_name)}">
	        <input type="submit" value="Upload file to your ${fn:escapeXml(customer_name)} Inventory">
        </form>	    
        <%
        if (invFile.isLoaded()) 
        {        
        %>
        <p>Also you can download your previously uploaded inventory as a file</p>
        <form action="/gcs" method="get">
            <input type="hidden" name="customer_name" value="${fn:escapeXml(customer_name)}"/>
            <input type="submit" value="Download your ${fn:escapeXml(customer_name)} Inventory">
        </form>
        <p>or</p>
        <%
        }
        %>
        <p>download test data as a file</p>
        <form action="/gcs" method="get">
            <input type="hidden" name="customer_name" value="TestInventory"/>
            <input type="submit" value="Download Test Inventory">
        </form>
        <%
        invState.close();
    }
%>
<p>For instruction on prepearing inventory data <a href="https://docs.google.com/document/d/1ZeFuV26SdLloWcDnlThMhlVhj4IZlalGfmwwo50pilI/pub" target="_blank">read White Paper</a></p>
</body>
</html>
