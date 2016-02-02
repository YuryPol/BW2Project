<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<%@ page import="com.google.appengine.api.users.User" %>
<%@ page import="com.google.appengine.api.users.UserService" %>
<%@ page import="com.google.appengine.api.users.UserServiceFactory" %>
<%@ page import="java.util.List" %>
<%@ taglib prefix="fn" uri="http://java.sun.com/jsp/jstl/functions" %>

<html>
<head>
    <link type="text/css" rel="stylesheet" href="/stylesheets/main.css"/>
</head>

<body>

<%
    String inventoryName = request.getParameter("inventoryName");
    if (inventoryName == null) {
        inventoryName = "default";
    }
    pageContext.setAttribute("inventoryName", inventoryName);
    UserService userService = UserServiceFactory.getUserService();
    User user = userService.getCurrentUser();
    if (user != null) {
        pageContext.setAttribute("user", user);
%>

<p>Hello, ${fn:escapeXml(user.nickname)}! (You can
    <a href="<%= userService.createLogoutURL(request.getRequestURI()) %>">sign out</a>.)</p>
<%
} else {
%>
<p>You can 
    <a href="<%= userService.createLoginURL(request.getRequestURI()) %>">sign in</a>
    with your Google account.</p>
<%
    }
%>


<form action="/start.jsp" method="get">
    <div><input type="text" name="inventoryName" value="${fn:escapeXml(inventoryName)}"/></div>
    <div><input type="submit" value="Switch Inventory"/></div>
</form>

</body>
</html>