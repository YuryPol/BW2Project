<%@ page import="com.bwing.invmanage2.InventoryUser" %>
<%@ page import="com.bwing.invmanage2.Customer" %>
<%@ page import="com.googlecode.objectify.ObjectifyService" %>
<%@ page import="com.googlecode.objectify.Key" %>
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
	        <p>work with Test inventory,</p>
	        <form action="/gcs" method="get">
	        <div><input type="submit" value="Test Inventory"/></div>
	        <input type="hidden" name="inventory" value="test"/>
        </form>
        <p>work with Your inventory</p>
        <form action="/gcs" method="get">
	        <div><input type="submit" value="Your Inventory"/></div>
	        <input type="hidden" name="inventory" value="custom"/>
	        <input type="hidden" name="customer_name" value="${fn:escapeXml(customer_name)}"/>
        </form>
        <p>or upload a new inventory</p>
        <form action="/gcs" method="post">
	        <div><input type="submit" value="Upload Inventory ${fn:escapeXml(customer_name)}"/></div>
	        <input type="hidden" name="inventory" value="upload"/>
	        <input type="hidden" name="customer_name" value="${fn:escapeXml(customer_name)}"/>
	        <input type="text" name="filepath" value="" maxlength="200" />
        </form>
        <%
    }
%>
</body>
</html>
