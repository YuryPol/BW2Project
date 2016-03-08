<%@ page import="com.bwing.invmanage2.InventoryUser" %>
<%@ page import="com.bwing.invmanage2.Customer" %>
<%@ page import="com.googlecode.objectify.ObjectifyService" %>
<%@ page import="com.googlecode.objectify.Key" %>
<%@ page contentType="text/html;charset=UTF-8" language="java" %>

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
        %>
        <p>You can return to starting page,</p>
        <form action="/" method="get">
        <div><input type="submit" value="Return"/></div>
        </form>
        <p>work with Test inventory,</p>
         <form action="/" method="get">
        <div><input type="submit" value="Test Inventory"/></div>
        </form>
        <p>work with Your inventory</p>
         <form action="/" method="get">
        <div><input type="submit" value="Your Inventory"/></div>
        </form>
        <p>or upload a new inventory</p>
        <form action="/" method="get">
        <div><input type="submit" value="Upload Inventory"/></div>
        </form>
        <%
    }
%>
</body>
</html>
