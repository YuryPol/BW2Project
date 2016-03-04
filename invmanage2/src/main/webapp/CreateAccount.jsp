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
<%@ page import="com.googlecode.objectify.Ref" %>
<%@ page import="java.util.List" %>
<%@ page import="java.util.Iterator" %>
<%@ taglib prefix="fn" uri="http://java.sun.com/jsp/jstl/functions" %>
<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c" %>  

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
                // .filter("user_email", theUser.getEmail())
                .list();
        InventoryUser iuser = InventoryUser.findInventoryUser(users, theUser.getEmail());
        if (iuser == null)
        {
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
                        // .filter("customer=", theCustomer)
                        .list();
                    if (!customers.isEmpty() && Customer.findCustomer(customers, company) != null)
                    {
                        // Customer already exists.
                        message = "Your company already has an account. Contact the owner " + customers.get(0).founder.getEmail()
                        + ". If you don't recognise the owner and want to proceed modify your company name (for now) and email us so we can clarify the conflict with imposter";
                    }
                    else
                    {
                        // Create Customer 
                        // TODO: make it into transacton 
                        Customer customer = new Customer(company, theUser);
                        ObjectifyService.ofy().save().entity(customer).now();
                        // Add the user and fill his properties
                        InventoryUser newuser = new InventoryUser(Ref.create(customer), theUser, first_name, last_name, new PhoneNumber(phone), theUser.getEmail());
                        ObjectifyService.ofy().save().entity(newuser).now();
                    }
                }
            }
            else
            {
                // User didn't submit data yet
                message = "Please enter your information";
            }
            %>
            <p>${fn:escapeXml(theuser.nickname)}!, <% out.println(message); %></p>
            <p>You can <a href="<%= userService.createLogoutURL(request.getRequestURI()) %>">sign out</a></p>
            <p>or enter your data</p>
            <form action="/CreateAccount.jsp" method="get">
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
            // registered user, let her chose/create inventory
            %>
            <p>Thank you for registering</p>
            <p>Now you can <a href="<%= userService.createLogoutURL(request.getRequestURI()) %>">sign out,</a></p>
            <p>or work with available inventories, or upload a new inventory</p>
            <form action="/SelectInventory.jsp" method="get">
            <div><input type="submit" value="Get Inventory"/></div>
            </form>
            <%
        }
    }
    else
    {
    	// Return to Start.jsp
    	response.sendRedirect(request.getContextPath() + "/Start.jsp");
//         String redirectURL = "/Start.jsp";
//         response.sendRedirect(redirectURL);
//            <c:redirect url="/Start.jsp"/>
    }
    %>

    </body>
    </html>
    
