<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<%@ page import="com.google.appengine.api.users.User" %>
<%@ page import="com.google.appengine.api.users.UserService" %>
<%@ page import="com.google.appengine.api.users.UserServiceFactory" %>
<%@ page import="com.bwing.invmanage2.InventoryUser" %>

<html>
<head>
    <title>Confirm created user account</title>
    <link type="text/css" rel="stylesheet" href="/stylesheets/main.css"/>
</head>
<body>
    <p>Thank you for creating user account</p>
    <%
    UserService userService = UserServiceFactory.getUserService();
    %>
    <p>You can <a href="<%= userService.createLogoutURL(request.getRequestURI()) %>">sign out,</a></p>
    <%
    // check if the user was created
    InventoryUser iuser = InventoryUser.getCurrentUser();
    if (iuser != null)
    {
	    %>
	    <p>or work with available inventories, or upload a new inventory</p>
	    <form action="/" method="get">
	    <div><input type="submit" value="Return to start page"/></div>
	    </form>
	    <%
    }
    else
    {
    	%>
        <p>or wait until we save your data</p>
    	<%
        response.setIntHeader("Refresh", 3);
    }
    %>
</body>
</html>