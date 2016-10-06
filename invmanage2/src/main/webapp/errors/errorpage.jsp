<%@ page import="com.bwing.invmanage2.InventoryUser" %>
<%@ page language="java" contentType="text/html; charset=ISO-8859-1" pageEncoding="ISO-8859-1"%>
<%@ page import="java.util.logging.Logger" %>

<%@ page isErrorPage="true" %>

<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
    <link type="text/css" rel="stylesheet" href="/stylesheets/main.css"/>
</head>
<body>

</body>
	<%
    Logger log = Logger.getLogger(this.getClass().getName());
    // check the user
    InventoryUser iuser = InventoryUser.getCurrentUser();
    if (iuser == null)
    {
    %>
	<p>ERROR! You were logged out</p>
	<p>Return to login page</p>
	<%
    }
    else
    {
        // Analyze the servlet exception       
        Throwable throwable = (Throwable)request.getAttribute("javax.servlet.error.exception");
        Integer statusCode = (Integer)request.getAttribute("javax.servlet.error.status_code");
        String servletName = (String)request.getAttribute("javax.servlet.error.servlet_name");
        if (servletName == null){
           servletName = "Unknown";
        }
        String requestUri = (String)request.getAttribute("javax.servlet.error.request_uri");
        if (requestUri == null){
           requestUri = "Unknown";
        }
    
        String message = "no errors found";
        if (throwable != null)
        {
            message = "ERROR! exception " + throwable.getMessage();
        }
        else if (statusCode != null)
        {
            message = "ERROR! The status code : " + statusCode;
        }
        else
        {
            message = "ERROR! The request URI: " + requestUri;
        }
        log.severe(message);
   	%> 
   	<p>ERROR: <%=message%></p>
   	<%
    }
    %>
	<form action="/" method="get">
		<div>
			<input type="submit" value="Return"/>
		</div>
	</form>
</body>
</html>