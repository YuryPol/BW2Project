<%@ page import="com.bwing.invmanage2.Greeting" %>
<%@ page import="com.bwing.invmanage2.Guestbook" %>
<%@ page import="com.googlecode.objectify.Key" %>
<%@ page import="com.bwing.invmanage2.InventoryUser" %>
<%@ page import="com.googlecode.objectify.ObjectifyService" %>
<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<%@ page import="com.google.appengine.api.users.User" %>
<%@ page import="com.google.appengine.api.users.UserService" %>
<%@ page import="com.google.appengine.api.users.UserServiceFactory" %>
<%@ page import="java.util.List" %>
<%@ page import="java.util.Iterator" %>
<%@ taglib prefix="fn" uri="http://java.sun.com/jsp/jstl/functions" %>

<html>
<head>
    <link type="text/css" rel="stylesheet" href="/stylesheets/main.css"/>
</head>

<body>

<%
	String guestbookName = "";
    UserService userService = UserServiceFactory.getUserService();
    User theUser = userService.getCurrentUser();
	if (theUser != null) {
		pageContext.setAttribute("theuser", theUser);
		// Create inventory user if necessary
		List<InventoryUser> users = ObjectifyService.ofy().load().type(InventoryUser.class) // We want only Users
				.filter("user", theUser)
				.list();
		if (users.isEmpty()) {
				// Create InventoryUser and Customer
			%>

			<p>Hello, ${fn:escapeXml(theuser.nickname)}! (You can
			    <a href="<%= userService.createLogoutURL(request.getRequestURI()) %>">sign out</a>.)</p>
			    <p>or enter your data</p>
			<%
		}
		else {
			if (users.get(0).user != theUser)
				// shouldn't ever happen
				;
			else
		        pageContext.setAttribute("inventoryuser", users.get(0));			
		}
    } 
	else 
	{
%>
<p>You can 
    <a href="<%= userService.createLoginURL(request.getRequestURI()) %>">sign in</a>
    with your Google account.</p>
<%
    }
%>


<form action="/start.jsp" method="get">
    <div><input type="text" name="guestbookName" value="${fn:escapeXml(guestbookName)}"/></div>
    <div><input type="submit" value="Switch Inventory"/></div>
</form>

</body>
</html>