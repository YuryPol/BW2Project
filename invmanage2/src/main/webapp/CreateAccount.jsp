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
        // Create inventory user if necessary
        List<InventoryUser> users = ObjectifyService.ofy().load().type(InventoryUser.class) // We want only Users
                // .filter("user_email", theUser.getEmail())
                .list();
        InventoryUser iuser = InventoryUser.findInventoryUser(users, theUser.getEmail());
            // Create InventoryUser and Customer
            String user_data_submited = request.getParameter("user_data_submited");
            if (user_data_submited != null) 
            {
                // user just submited her info
                String email = "";
                String first_name = request.getParameter("first_name");
                pageContext.setAttribute("first_name", first_name);
                String last_name = request.getParameter("last_name");
                pageContext.setAttribute("last_name", last_name);
                String phone = request.getParameter("phone");
                pageContext.setAttribute("phone", phone);
                String company;
                if (iuser == null) 
               	{
                    // Creating primary account
                	company = request.getParameter("company");
               	}
                else
                {
                    // Creating account for another user
                	company = iuser.theCustomer.get().company;
                    email = request.getParameter("email");
                }
                
                if (first_name == "" || last_name == "" || phone == "" || company == "") // TODO: add more checks
                {
                    message = "please enter missing data";
                }
                else if (iuser == null)
                {
                    // Create primary user (owner)
                    // Create the correct Ancestor key
                    Key<Customer> theCustomer = Key.create(Customer.class, company);
                    // Run an ancestor query to ensure that this company isn't recorded yet in our datastore.
                    List<Customer> customers = ObjectifyService.ofy().load().type(Customer.class) // We want only Customers
                        // .filter("customer=", theCustomer)
                        .list();
                    if (!customers.isEmpty() && Customer.findCustomer(customers, company) != null)
                    {
                        // Customer already exists.
                        message = "Your company already has a service with us. Contact " 
                        + customers.get(0).founder.getEmail()
                        + " so s/he can add your user account."
                        + " If you don't recognise the owner and still want to proceed modify your company name (for now)"
                        + " and email us at admin@baterflywing.com so we can clarify the conflict.";
                    }
                    else
                    {
                        // Create Customer and primary user
                        // TODO: make it into transacton 
                        Customer customer = new Customer(company, theUser);
                        ObjectifyService.ofy().save().entity(customer).now();
                        // Add the user and fill his properties
                        InventoryUser newuser = new InventoryUser(Ref.create(customer), first_name, last_name, new PhoneNumber(phone), new Email(theUser.getEmail()));
                        ObjectifyService.ofy().save().entity(newuser).now();
                        response.sendRedirect("/ConfirmAccount.jsp"); 
                   }
                }
                else
                {
                	// Create secondary user
                    // Add the user and fill his properties
                    message = "Add a new user with whom you will share your company data";
                    Key<Customer> customerKey = InventoryUser.getCurrentUser().getCustomerKey();
                    InventoryUser newuser = new InventoryUser(Ref.create(customerKey), first_name, last_name, new PhoneNumber(phone), new Email(email));
                    ObjectifyService.ofy().save().entity(newuser).now();
                    response.sendRedirect("/ConfirmAccount.jsp"); 
                }
            }
            else
            {
                // User didn't submit data yet
                message = "Please enter the new user's information";
            }
            %>
            <p>${fn:escapeXml(theUser.email)}!, <% out.println(message); %></p>
            <p>You can <a href="<%= userService.createLogoutURL(request.getRequestURI()) %>">sign out</a></p>
            <p>or enter identifying information</p>
            <form>
                <%
                if (iuser != null) {
                %>
                <div>email<input type="email" name="email" value="${fn:escapeXml(email)}" required/></div>
                <%
                }
                %>
                <div>First name <input type="text" name="first_name" value="${fn:escapeXml(first_name)}" required/></div>
                <div>Last name <input type="text" name="last_name" value="${fn:escapeXml(last_name)}" required/></div>
                <div>Phone number <input type="text" name="phone" value="${fn:escapeXml(phone)}" required/></div>
                <%
                if (iuser == null) {
                %>
                <div>Company <input type="text" name="company" value="${fn:escapeXml(company)}" required/></div>
                <%
                }
                %>
                <input type="hidden" name="user_data_submited" value="user_data_submited"/>
                <div><input type="submit" value="Submit"/></div>
            </form>
            <%
    }
    else
    {
    	// Return to Start.jsp
    	response.sendRedirect(request.getContextPath() + "/start.jsp");
    }
    %>

    </body>
    </html>
    
