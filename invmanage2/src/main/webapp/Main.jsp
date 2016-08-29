<%@ page import="com.bwing.invmanage2.UIhelper" %>
<%@ page import="com.bwing.invmanage2.InventoryUser" %>
<%@ page import="com.bwing.invmanage2.InventoryState" %>
<%@ page import="com.bwing.invmanage2.InventoryFile" %>
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
//         List<InventoryUser> users = ObjectifyService.ofy().load().type(InventoryUser.class) // We want only Users
//                 // .filter("user_email", gUser.getEmail())
//                 .list();
        InventoryUser iuser = InventoryUser.getGmailUser(gUser.getEmail());
        if (iuser == null)
        {
        	// The user wasn't registered
        	%>
                <p>Logged in as <%= gUser.getNickname()%></p>
        	<%
        	String mode = request.getParameter("mode");
        	if (mode != null && mode.equals("createAccount"))
        	{
        		%>
	            <p>You can enter identifying information to create user account</p>
                <form action="/" method="post">
                <div>Business email<input type="email" name="bis_email" value="${fn:escapeXml(bis_email)}" required/></div>
                <div>First name <input type="text" name="first_name" value="${fn:escapeXml(first_name)}" required/></div>
                <div>Last name <input type="text" name="last_name" value="${fn:escapeXml(last_name)}" required/></div>
                <div>Phone number <input type="tel" name="phone" value="${fn:escapeXml(phone)}" required/></div>
                <div>Organization <input type="text" name="company" value="${fn:escapeXml(company)}" required/></div>
                <input type="hidden" name="mode" value="user_data_submited"/>
                <input type="submit" value="Create Account"/> By creating the account you accept the Terms of Service below
                </form>
	            <% 
        	}
        	else if (mode != null && mode.equals("user_data_submited"))
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
                    <p>Please enter correct data</p>
	                <form action="/" method="post">
	                <div>Business email<input type="email" name="bis_email" value="${fn:escapeXml(bis_email)}" required/></div>
	                <div>First name <input type="text" name="first_name" value="${fn:escapeXml(first_name)}" required/></div>
	                <div>Last name <input type="text" name="last_name" value="${fn:escapeXml(last_name)}" required/></div>
	                <div>Phone number <input type="tel" name="phone" value="${fn:escapeXml(phone)}" required/></div>
	                <div>Organization <input type="text" name="company" value="${fn:escapeXml(company)}" required/></div>
	                <input type="hidden" name="mode" value="user_data_submited"/>
	                <input type="submit" value="Create Account"/> By creating the account you accept the Terms of Service below                
	                </form>
                    <%
                }
                else 
                {
                    // Create Customer and primary user
                    // TODO: make it into transacton 
                    Customer customer = new Customer(company, gUser);
                    ObjectifyService.ofy().save().entity(customer).now();
                    // Add the user and fill his properties
                    InventoryUser newuser = new InventoryUser(Ref.create(customer), new Email(gUser.getEmail()), first_name, last_name, new PhoneNumber(phone), bis_email, true);
                    ObjectifyService.ofy().save().entity(newuser).now();
                    //response.sendRedirect("/ConfirmAccount.jsp"); 
                }
        	}
        	else
        	{
        		%>
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
            String customer_name = iuser.theCustomer.get().company;
            %>
            <p>Logged in as <%=iuser.user_first_name%> <%=iuser.user_last_name %> from <%=iuser.theCustomer.get().company %></p>
            <%
            pageContext.setAttribute("customer_name", customer_name);
            String modeStr = request.getParameter("mode");
            UIhelper.Mode mode;
            if (modeStr == null)
            	mode = UIhelper.Mode.none;
            else
            	mode = UIhelper.Mode.valueOf(modeStr);
            	
            switch (mode) 
            {
	            case createAccount:
	                %>
	                <p>You can enter identifying information to create account for secondary user with whom you share your organization's data</p>
	                <form action="/" method="post">
                    <div>g-mail for login<input type="email" name="email" value="${fn:escapeXml(email)}" required/></div>
	                <div>Business email<input type="email" name="bis_email" value="${fn:escapeXml(bis_email)}" required/></div>
	                <div>First name <input type="text" name="first_name" value="${fn:escapeXml(first_name)}" required/></div>
	                <div>Last name <input type="text" name="last_name" value="${fn:escapeXml(last_name)}" required/></div>
	                <div>Phone number <input type="tel" name="phone" value="${fn:escapeXml(phone)}" required/></div>
	                <input type="hidden" name="mode" value="user_data_submited"/>
	                <input type="submit" value="Create Account"/> By creating the account you accept the Terms of Service below
	                </form>
	                <% 
	                break;
                case user_data_submited:
                    // user just submited her info
                    String message = "";
                    String email = request.getParameter("email");
                    String first_name = request.getParameter("first_name");
                    String last_name = request.getParameter("last_name");
                    String phone = request.getParameter("phone");
                    String bis_email = request.getParameter("bis_email");
                     
                    if (first_name == "Jerk" || last_name == "" || phone == "" || bis_email == "") // TODO: add real parameters' checks
                    {
                        %>
                        <p>Please correct wrong data</p>
	                    <form action="/" method="post">
	                    <div>g-mail for login<input type="email" name="email" value="${fn:escapeXml(email)}" required/></div>
	                    <div>Business email<input type="email" name="bis_email" value="${fn:escapeXml(bis_email)}" required/></div>
	                    <div>First name <input type="text" name="first_name" value="${fn:escapeXml(first_name)}" required/></div>
	                    <div>Last name <input type="text" name="last_name" value="${fn:escapeXml(last_name)}" required/></div>
	                    <div>Phone number <input type="tel" name="phone" value="${fn:escapeXml(phone)}" required/></div>
	                    <input type="hidden" name="mode" value="user_data_submited"/>
	                    <input type="submit" value="Create Account"/> By creating the account you accept the Terms of Service below
	                    </form>
                        <%
                    }
                    else 
                    {
                        // Create secondary user
                        // Add the user and fill his properties
                        Ref<Customer> customerKey = iuser.theCustomer;
                        InventoryUser newuser = new InventoryUser(customerKey, new Email(email), first_name, last_name, new PhoneNumber(phone), bis_email, false);
                        ObjectifyService.ofy().save().entity(newuser).now();
                        response.sendRedirect("/"); 
                    }
                    break;
                default:
                    // registered user, let her chose/create inventory
                    InventoryState invState = new InventoryState(customer_name, true);
                    if (invState.isLoaded() && invState.hasData())
                    {
                        %>
                        <form action="/" method="post">
                        <input type="hidden" name="mode" value="Allocate"/>
                        Allocate impressions from the inventory <input type="submit" value="Allocate"/>
                        </form>
                        <p>Or you can start over and re-initialize your inventory with new data</p>
                        <%
                    }
                    else if (invState.isWrongFile())
                    {
                        %>
                        <p><font color="red">Uploaded inventory file has a wrong format. Upload correct file and re-initialize the inventory</font></p>
                        <%
                    }
                    else 
                    {
                        %>
                        <p><font color="red">The inventory data was not initialized</font></p>
                        <%
                    }
                    InventoryFile testFile = new InventoryFile("TestInventory");
                    if (testFile.isLoaded())
                    {
                    %>        
                        <form action="/load" method="get">
                        <input type="hidden" name="file_name" value="TestInventory"/>
                        <input type="hidden" name="customer_name" value="${fn:escapeXml(customer_name)}"/>
                        Initialize inventory with test data <input type="submit" value="Initialize Test Inventory"/>
                        </form>        
                    <%
                    }
                    else
                    {
                    %>
                        <p><font color="red">Test inventory file was not found</font></p>
                    <%
                    }
                    InventoryFile invFile = new InventoryFile(customer_name);
                    if (invFile.isLoaded() && !invState.isWrongFile()) 
                    {        
                    %>
                        <form action="/load" method="get">
                        <input type="hidden" name="file_name" value="${fn:escapeXml(customer_name)}"/>
                        <input type="hidden" name="customer_name" value="${fn:escapeXml(customer_name)}"/>
                        Initialize inventory with data you uploaded <input type="submit" value="Initialize ${fn:escapeXml(customer_name)} Inventory"/>
                        </form>
                    <%  
                    }
                    %>        
                    <form action="/gcs" method="post" enctype="multipart/form-data">
                        Upload a new inventory data file <input type="file" name="${fn:escapeXml(customer_name)}">
                        <input type="submit" value="Upload file for your ${fn:escapeXml(customer_name)} Inventory">
                    </form>     
                    <%
                    if (invFile.isLoaded()) 
                    {        
                    %>
                    <form action="/gcs" method="get">
                    <input type="hidden" name="customer_name" value="${fn:escapeXml(customer_name)}"/>
                    Also you can download your previously uploaded inventory as a file <input type="submit" value="Download your ${fn:escapeXml(customer_name)} Inventory">
                    </form>
                    <%
                    }
                    if (iuser.isPrimary)
                    {
                    %>
                    <form action="/" method="post">
                    <input type="hidden" name="mode" value="createAccount"/>
                    Create or modify a secondary user account for your organization <input type="submit" value="Create account"/>
                    </form>
                    <%
                    }
                    invState.close();
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
	    <p><a href="<%= userService.createLoginURL(request.getRequestURI()) %>">Sign in</a> with your Google email.</p>
	    <%
    }
    %>
	<p><a href="/BookAdvertisingCampaignsInstantly.html" target="_blank">Read White Paper</a></p>
	<p><a href="/EUA.html" target="_blank">Read Terms of Service</a></p>
</body>
</html>