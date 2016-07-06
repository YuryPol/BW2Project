<%@ page import="com.bwing.invmanage2.InventoryUser" %>
<%@ page import="com.googlecode.objectify.Key" %>
<%@ page import="com.bwing.invmanage2.InventoryUser" %>
<%@ page import="com.bwing.invmanage2.Customer" %>
<%@ page import="com.googlecode.objectify.ObjectifyService" %>
<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<%@ page import="com.google.appengine.api.users.User" %>
<%@ page import="com.google.appengine.api.users.UserService" %>
<%@ page import="com.google.appengine.api.users.UserServiceFactory" %>
<%@ page import="com.google.appengine.api.datastore.PhoneNumber" %>
<%@ page import="com.googlecode.objectify.Ref" %>
<%@ page import="java.util.List" %>
<%@ page import="java.util.Iterator" %>
<%@ taglib prefix="fn" uri="http://java.sun.com/jsp/jstl/functions" %>

<html>
<head>
    <link type="text/css" rel="stylesheet" href="/stylesheets/main.css"/>
</head>

<body>

<%
    UserService userService = UserServiceFactory.getUserService();
    User theUser = userService.getCurrentUser();
	if (theUser != null) {
		pageContext.setAttribute("theuser", theUser);
		// Create inventory user if necessary
		List<InventoryUser> users = ObjectifyService.ofy().load().type(InventoryUser.class) // We want only Users
				// .filter("user_email", theUser.getEmail())
				.list();
		InventoryUser iuser = InventoryUser.findInventoryUser(users, theUser.getEmail());
		if (iuser == null)
		{
			// Create account
			%>
            <p>Hi ${fn:escapeXml(theuser.nickname)}!</p>
            <p>You can <a href="<%= userService.createLogoutURL(request.getRequestURI()) %>">sign out,</a></p>
            <p>or create a user account</p>
            <form action="/CreateAccount.jsp" method="get">
            <input type="submit" value="Create account"/>
            </form>
            <%
		}
		else 
		{
			// registered user, let her chose/create inventory
	        %>
	        <p>Hi <%= iuser.user_first_name + " " + iuser.user_last_name %></p>
            <p>You can <a href="<%= userService.createLogoutURL(request.getRequestURI()) %>">sign out,</a></p>
            <p>or add another user account for your organization</p>
            <form action="/CreateAccount.jsp" method="get">
            <input type="submit" value="Add another user"/>
            </form>            
            <p>or you can work with available inventories, or upload a new inventory</p>
	        <form action="/SelectInventory.jsp" method="get">
	        <div><input type="submit" value="Get Inventory"/></div>
	        </form>
	        <%
		}
    } 
	else 
	{
%>
<p>You can <a href="<%= userService.createLoginURL(request.getRequestURI()) %>">sign in</a> with your Google email.</p>
<%
    }
%>
<p><a href="https://docs.google.com/document/d/1ZeFuV26SdLloWcDnlThMhlVhj4IZlalGfmwwo50pilI/pub" target="_blank">Read White Paper</a></p>
<p><a href="https://docs.google.com/document/d/1ma2MWrMIqV8CUuFqa_9kPUzDEcGGUWJ27lPOpJyXzJA/pub" target="_blank">Read Terms of Service</a></p>
</body>
</html>