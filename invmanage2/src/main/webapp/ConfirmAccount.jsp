<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<%@ page import="com.google.appengine.api.users.User" %>
<%@ page import="com.google.appengine.api.users.UserService" %>
<%@ page import="com.google.appengine.api.users.UserServiceFactory" %>

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
    <p>Now you can <a href="<%= userService.createLogoutURL(request.getRequestURI()) %>">sign out,</a></p>
    <p>or work with available inventories, or upload a new inventory</p>
    <form action="/" method="get">
    <div><input type="submit" value="Return to main page"/></div>
    </form>
</body>
</html>