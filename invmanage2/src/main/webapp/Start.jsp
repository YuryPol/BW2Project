<%@ page import="com.bwing.invmanage2.InventoryUser" %>
<%@ page import="com.bwing.invmanage2.Guestbook" %>
<%@ page import="com.googlecode.objectify.Key" %>
<%@ page import="com.bwing.invmanage2.InventoryUser" %>
<%@ page import="com.bwing.invmanage2.Customer" %>
<%@ page import="com.googlecode.objectify.ObjectifyService" %>
<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<%@ page import="com.google.appengine.api.users.User" %>
<%@ page import="com.google.appengine.api.users.UserService" %>
<%@ page import="com.google.appengine.api.users.UserServiceFactory" %>
<%@ page import="com.google.appengine.api.datastore.PhoneNumber" %>
<%@ page import="java.util.List" %>
<%@ page import="java.util.Iterator" %>
<%@ taglib prefix="fn" uri="http://java.sun.com/jsp/jstl/functions" %>

<html>
<head>
    <link type="text/css" rel="stylesheet" href="/stylesheets/main.css"/>
</head>

<body>

<%
    String message = "";
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
		    String user_data_submited = request.getParameter("user_data_submited");
		    if (user_data_submited != null) 
		    {
		        // user just submited here info
		        String first_name = request.getParameter("first_name");
		        pageContext.setAttribute("first_name", first_name);
		        String last_name = request.getParameter("last_name");
		        pageContext.setAttribute("last_name", last_name);
		        String phone = request.getParameter("phone");
		        pageContext.setAttribute("phone", phone);
		        String company = request.getParameter("company");
		        pageContext.setAttribute("company", company);
		        if (first_name == "" || last_name == "" || phone == "" || company == "")
		        {
		        	message = "please enter missing data";
		        }
		        else
		        {
		        	// Create primary owner
		            // Create the correct Ancestor key
		            Key<Customer> theCustomer = Key.create(Customer.class, company);
		            // Run an ancestor query to ensure that this company isn't recorded yet in our datastore.
		            List<Customer> customers = ObjectifyService.ofy().load().type(Customer.class) // We want only Customers
		                .filter("customer=", theCustomer)
		                .list();
		            if (!customers.isEmpty())
		            {
		            	// Customer already exists.
		            	message = "Your company already has an account. Contact the owner " + customers.get(0).founder.getEmail()
		            	+ ". If you don't recognise the owner and want to proceed modify your company name (for now) and email us so we can clarify the conflict with imposter";
		            }
		            else
		            {
		            	// Create Customer
		            	Customer customer = new Customer(company, theUser);
		            	// Fill user properties
		            	InventoryUser iuser = new InventoryUser(customer..company, first_name, last_name, new PhoneNumber(phone));
		            }
		        }
		    }
		    else
		    {
		    	// User didn't submit data yet
		    	message = "Please enter your information";
		    }
		}
		else {
			if (users.get(0).user != theUser) 
			{
				// shouldn't ever happen
				%>
                <p>Wrong user. Please <a href="<%= userService.createLogoutURL(request.getRequestURI()) %>">sign out now!</a></p>
				<%
			}
			else 
			{
				// known user, let her chose/create inventory
		        pageContext.setAttribute("inventoryuser", users.get(0));
		        %>
		        <form action="/start.jsp" method="get">
		        <div><input type="text" name="inventory" value="${fn:escapeXml(inventory)}"/></div>
		        <div><input type="submit" value="Switch Inventory"/></div>
		        </form>
		        <%
			}
		}
		%>
		<p>${fn:escapeXml(theuser.nickname)}!, "please enter missing data"</p>
		<p>You can <a href="<%= userService.createLogoutURL(request.getRequestURI()) %>">sign out</a></p>
		<p>or enter your data</p>
		<form action="/start.jsp" method="get">
		    <div>First name <input type="text" name="first_name" value="${fn:escapeXml(first_name)}"/></div>
		    <div>Last name <input type="text" name="last_name" value="${fn:escapeXml(last_name)}"/></div>
		    <div>Phone number <input type="text" name="phone" value="${fn:escapeXml(phone)}"/></div>
		    <div>Company <input type="text" name="company" value="${fn:escapeXml(company)}"/></div>
		    <input type="hidden" name="user_data_submited" value="user_data_submited"/>
		    <div><input type="submit" value="Submit your data"/></div>
		</form>
		<%
    } 
	else 
	{
%>
<p>You can 
    <a href="<%= userService.createLoginURL(request.getRequestURI()) %>">sign in</a> with your Google account.</p>
<%
    }
%>

</body>
</html>