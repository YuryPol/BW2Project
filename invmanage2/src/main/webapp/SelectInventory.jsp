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
        System.out.println("Customer: " + customer_name);
        pageContext.setAttribute("customer_name", customer_name);
        %>
        <p>You can return to starting page,</p>
        <form action="/" method="get">
        <div><input type="submit" value="Return"/></div>
        </form>
        
        <% 
        try (InventoryState invState = new InventoryState(customer_name))
        {
        	if (invState.isLoaded())
        	{
        		%>
		        <p>Allocate items in your inventory,</p>
		        <form action="/Allocate.jsp" method="get">
		        <div><input type="submit" value="Allocate"/></div>
		        </form>
        		<%
        	}
        }
        %>
        
        <p>Initialize inventory with test data</p>
	    <form action="/load" method="get">
	        <div><input type="submit" value="Test Inventory"/></div>
            <input type="hidden" name="customer_name" value="TestInventory"/>
        </form>
        
        <%
        InventoryFile invFile = new InventoryFile(customer_name);
        if (invFile.isLoaded()) 
        {        
        %>
	        <p>Initialize  inventory with your data</p>
	        <form action="/load" method="get">
		        <div><input type="submit" value="Your Inventory"/></div>
		        <input type="hidden" name="customer_name" value="${fn:escapeXml(customer_name)}"/>
	        </form>
	    <% 
	    }
	    %>
        
        <p>Upload a new inventory file</p>
	    <form action="/gcs" method="post" enctype="multipart/form-data">
	        <input type="file" name="${fn:escapeXml(customer_name)}">
	        <input type="submit" value="Upload file to your ${fn:escapeXml(customer_name)} Inventory">
	    </form>
	    
        <p>Or just download your inventory as a file</p>
        <form action="/gcs" method="get">
            <input type="hidden" name="customer_name" value="${fn:escapeXml(customer_name)}"/>
            <input type="submit" value="Download your ${fn:escapeXml(customer_name)} Inventory">
        </form>
        <%
    }
%>
</body>
</html>
