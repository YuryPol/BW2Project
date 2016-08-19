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
<%@ page import="com.google.appengine.api.datastore.Email" %>
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
    User gUser = userService.getCurrentUser();
    if (gUser != null) {
    	// user was looged in
        // try to find gUser in Inventry users
        List<InventoryUser> users = ObjectifyService.ofy().load().type(InventoryUser.class) // We want only Users
                // .filter("user_email", gUser.getEmail())
                .list();
        InventoryUser iuser = InventoryUser.findInventoryUser(users, gUser.getEmail());
        if (iuser == null)
        {
        	// The user wasn't registered
        	if (request.getParameter("mode") == "createAccount")
        	{
        		%>
	            <p>You can enter identifying information to create user account</p>
                <form action="/" method="post">
                <div>Business email<input type="email" name="bis_email" value="${fn:escapeXml(bis_email)}" required/></div>
                <div>First name <input type="text" name="first_name" value="${fn:escapeXml(first_name)}" required/></div>
                <div>Last name <input type="text" name="last_name" value="${fn:escapeXml(last_name)}" required/></div>
                <div>Phone number <input type="tel" name="phone" value="${fn:escapeXml(phone)}" required/></div>
                <div>Organization <input type="text" name="company" value="${fn:escapeXml(company)}" required/></div>
                <input type="hidden" name="user_data_submited" value="user_data_submited"/>
                <div>
                <input type="submit" value="Create Account"/> By creating the account you accept the Terms of Service below                
                </div>
	            <% 
        	}
        	else if (request.getParameter("mode") == "user_data_submited")
        	{
                // user just submited her info
                String message = "";
                String email = "";
                String first_name = request.getParameter("first_name");
                String last_name = request.getParameter("last_name");
                String phone = request.getParameter("phone");
                String bis_email = request.getParameter("bis_email");
                String company = request.getParameter("company");
                
                if (first_name == "Jerk" || last_name == "" || phone == "" || company == "" || bis_email == "") // TODO: add real parameters' checks
                {
                    %>
                     <p>Please enter missing data</p>
                    <%
                }
                else 
                {
                    // Create Customer and primary user
                    // TODO: make it into transacton 
                    Customer customer = new Customer(company, gUser);
                    ObjectifyService.ofy().save().entity(customer).now();
                    // Add the user and fill his properties
                    InventoryUser newuser = new InventoryUser(Ref.create(customer), 
                    		first_name, last_name, new PhoneNumber(phone), new Email(gUser.getEmail()), true);
                    ObjectifyService.ofy().save().entity(newuser).now();
                    response.sendRedirect("/ConfirmAccount.jsp"); 
                }
        	}
        	else
        	{
        		%>
                <p>Hi <%= gUser.getNickname()%></p>
                <form action="/" method="post">
                <input type="hidden" name="mode" value="createAccount"/>
                You can create primary user account for your organization <input type="submit" value="Create account"/>
                </form>
                <%
        	}
        }
        else 
        {
            // registered user
            switch (request.getParameter("mode")) 
            {
	            case "createAccount":
	                %>
	                <p>You can enter identifying information to create secondary user account</p>
	                <form action="/" method="post">
                    <div>g-mail for login<input type="email" name="email" value="${fn:escapeXml(email)}" required/></div>
	                <div>Business email<input type="email" name="bis_email" value="${fn:escapeXml(bis_email)}" required/></div>
	                <div>First name <input type="text" name="first_name" value="${fn:escapeXml(first_name)}" required/></div>
	                <div>Last name <input type="text" name="last_name" value="${fn:escapeXml(last_name)}" required/></div>
	                <div>Phone number <input type="tel" name="phone" value="${fn:escapeXml(phone)}" required/></div>
	                <div>Organization <input type="text" name="company" value="${fn:escapeXml(company)}" required/></div>
	                <input type="hidden" name="user_data_submited" value="user_data_submited"/>
	                <div><input type="submit" value="Create Account"/> By creating the account you accept the Terms of Service below</div>
	                <% 
	            break;
                case "1":
                %>
                <%       
                break;
                case "2":
                %>
                <%       
                break;
                case "3":
                %>
                <%       
                break;
                case "4":
                %>
                <%       
                break;
            }
        }
        %>
            <p>Or you can <a href="<%= userService.createLogoutURL(request.getRequestURI()) %>">sign out</a></p>
        <%
    } 
    else 
    {
	    %>
	    <p>You can <a href="<%= userService.createLoginURL(request.getRequestURI()) %>">sign in</a> with your Google email.</p>
	    <%
    }
    %>
	<p><a href="/BookAdvertisingCampaignsInstantly.html" target="_blank">Read White Paper</a></p>
	<p><a href="/EUA.html" target="_blank">Read Terms of Service</a></p>
</body>
</html>