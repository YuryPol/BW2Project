<%@ page import="com.bwing.invmanage2.UIhelper" %>
<%@ page import="com.bwing.invmanage2.InventoryUser" %>
<%@ page import="com.bwing.invmanage2.InventoryState" %>
<%@ page import="com.bwing.invmanage2.InventoryState.Status" %>
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
<%@ page import="java.sql.ResultSet" %>
<%@ page import="java.sql.Statement" %>
<%@ page import="java.util.logging.Logger" %>
<%@ page import="java.util.logging.Level" %>
<%@ taglib prefix="fn" uri="http://java.sun.com/jsp/jstl/functions" %>
<%@ include file="PreventCache.jsp" %>

<%@ include file="Branding.jsp" %>

<html>
<head>
    <link type="text/css" rel="stylesheet" href="/stylesheets/main.css"/>
</head>

<body>

<%
    Logger log = Logger.getLogger(this.getClass().getName());
    log.setLevel(Level.INFO);
    UserService userService = UserServiceFactory.getUserService();
    User gUser = userService.getCurrentUser();
    String modeStr = request.getParameter("mode");
    UIhelper.Mode mode;
    if (modeStr == null)
        mode = UIhelper.Mode.none;
    else
        mode = UIhelper.Mode.valueOf(modeStr);

    if (gUser != null) {
    	// user was looged in
        // try to find gUser in Inventry users
        InventoryUser iuser = InventoryUser.getGmailUser(gUser.getEmail());
        if (iuser == null)
        {
        	// The user wasn't registered
        	%>
                <p>Logged in as <%= gUser.getNickname()%></p>
        	<%
            switch (mode) 
            {
                case createAccount:
                log.info("Entering account info by " + gUser.getNickname());
        		%>
	            <p>Enter identifying information to create user account</p>
                <form action="/" method="post">
                <div>Contact email<input type="email" name="bis_email" value="${fn:escapeXml(bis_email)}" /></div>
                <div>First name <input type="text" name="first_name" value="${fn:escapeXml(first_name)}" /></div>
                <div>Last name <input type="text" name="last_name" value="${fn:escapeXml(last_name)}" /></div>
                <div>Phone number <input type="tel" name="phone" value="${fn:escapeXml(phone)}" /></div>
                <div>Organization <input type="text" name="company" value="${fn:escapeXml(company)}" required/> This will be used to identify your dataset</div>
                <input type="hidden" name="mode" value="<%=UIhelper.Mode.user_data_submited.toString()%>"/>
                <input type="submit" value="Create Account"/> <!-- By creating the account you accept the Terms of Service below -->
                </form>
	            <%
	            break;
                case user_data_submited:
                // user just submited her info
                String message = "";
                String email = "";
                String first_name = request.getParameter("first_name");
                String last_name = request.getParameter("last_name");
                String phone = request.getParameter("phone");
                String bis_email = request.getParameter("bis_email");
                String company = request.getParameter("company").replaceAll("[^A-Za-z0-9]", "_");
                
                if (Customer.getCustomer(company) != null) 
                	// || first_name == "Jerk" || last_name == "" || phone == "" || company == "" || bis_email == "") // TODO: add real parameters' checks
                {
                    log.warning("Organization " + company + " already exists, tried to create by " + gUser.getNickname());
                    %>
                    <p>Organization <%=company%> already exists, try another organization name</p>
	                <form action="/" method="post">
	                <div>Contact email<input type="email" name="bis_email" value="${fn:escapeXml(bis_email)}" /></div>
	                <div>First name <input type="text" name="first_name" value="${fn:escapeXml(first_name)}" /></div>
	                <div>Last name <input type="text" name="last_name" value="${fn:escapeXml(last_name)}" /></div>
	                <div>Phone number <input type="tel" name="phone" value="${fn:escapeXml(phone)}" /></div>
	                <div>Organization <input type="text" name="company" value="${fn:escapeXml(company)}" required/> This will be used to identify your dataset</div>
	                <input type="hidden" name="mode" value="<%=UIhelper.Mode.user_data_submited.toString()%>"/>
	                <input type="submit" value="Create Account"/> <!-- By creating the account you accept the Terms of Service below -->                
	                </form>
                    <%
                }
                else 
                {
                    // Create Customer and primary user
                    // TODO: make it into transacton 
                    log.info("Creating Account for " + gUser.getNickname());
                    Customer customer = new Customer(company, gUser);
                    ObjectifyService.ofy().save().entity(customer).now();
                    // Add the user and fill his properties
                    InventoryUser newuser = new InventoryUser(Ref.create(customer), new Email(gUser.getEmail()), first_name, last_name, new PhoneNumber(phone), bis_email, true);
                    ObjectifyService.ofy().save().entity(newuser).now();
                    response.sendRedirect("/ConfirmAccount.jsp");
                    return;
                }
                break;
                default:
        		%>
                <form action="/" method="post">
                <input type="hidden" name="mode" value="<%=UIhelper.Mode.createAccount.toString()%>"/>
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
            <p>Logged in as <%= gUser.getNickname()%> from <%=iuser.theCustomer.get().company %></p>
            <%
            pageContext.setAttribute("customer_name", customer_name);
            	
            switch (mode) 
            {
	            case createAccount:
	                log.info(customer_name + " : Entering secondary Account info");
	                %>
	                <p>Enter identifying information to create account for secondary user with whom you share your organization's data</p>
	                <form action="/" method="post">
                    <div>g-mail for login<input type="email" name="email" value="${fn:escapeXml(email)}" required/></div>
	                <div>Contact email<input type="email" name="bis_email" value="${fn:escapeXml(bis_email)}" /></div>
	                <div>First name <input type="text" name="first_name" value="${fn:escapeXml(first_name)}" /></div>
	                <div>Last name <input type="text" name="last_name" value="${fn:escapeXml(last_name)}" /></div>
	                <div>Phone number <input type="tel" name="phone" value="${fn:escapeXml(phone)}" /></div>
	                <input type="hidden" name="mode" value="<%=UIhelper.Mode.user_data_submited.toString()%>"/>
	                <input type="submit" value="Create Account"/> <!-- By creating the account you accept the Terms of Service below -->
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
                     
                    if (InventoryUser.getGmailUser(email) != null) // TODO: add real parameters' checks
                    {
                        log.info(customer_name + " : Correcting secondary Account info");
                        %>
                        <p><font color="red">An account with this g-mail for login already exists</font></p>
                        <p>Please correct wrong data</p>
	                    <form action="/" method="post">
	                    <div>g-mail for login<input type="email" name="email" value="${fn:escapeXml(email)}" required/></div>
	                    <div>Contact email<input type="email" name="bis_email" value="${fn:escapeXml(bis_email)}" /></div>
	                    <div>First name <input type="text" name="first_name" value="${fn:escapeXml(first_name)}" /></div>
	                    <div>Last name <input type="text" name="last_name" value="${fn:escapeXml(last_name)}" /></div>
	                    <div>Phone number <input type="tel" name="phone" value="${fn:escapeXml(phone)}" /></div>
	                    <input type="hidden" name="mode" value="<%=UIhelper.Mode.user_data_submited.toString()%>"/>
	                    <input type="submit" value="Create Account"/> <!-- By creating the account you accept the Terms of Service below -->
	                    </form>
                        <%
                    }
                    else 
                    {
                        // Create secondary user
                        // Add the user and fill his properties
                        log.info(customer_name + " : Creating secondary Account " + first_name + " " + last_name);
                        Ref<Customer> customerKey = iuser.theCustomer;
                        InventoryUser newuser = new InventoryUser(customerKey, new Email(email), first_name, last_name, new PhoneNumber(phone), bis_email, false);
                        ObjectifyService.ofy().save().entity(newuser).now();
                        response.sendRedirect("/");
                        return;
                    }
                    break;
                case Allocate:
                    String set_name = request.getParameter("set_name");
                    int alloc_Amount = 0;
                    String advertiserID = "";
                    if (request.getParameter("alloc_Amount") != null)
                    {
                        alloc_Amount = Integer.parseInt(request.getParameter("alloc_Amount").trim());
                        advertiserID = request.getParameter("advertiserID").trim();
                    }
                    // System.out.println("Customer: " + customer_name);
                    pageContext.setAttribute("customer_name", customer_name);
                    InventoryState invState = new InventoryState(customer_name, true);
                    log.info(customer_name + " : The inventory is " + invState.getStatus());
                    if (!invState.isLoaded())
                    {
                        %>
                        <p>The inventory <%=customer_name%> is <%=invState.getStatus()%>>: Return to start page 
                        <form action="/" method="get">
                        <div><input type="submit" value="Return"/></div>
                        </form>
                        </p>
                        <%
                    }
                    else {
                    %>
                    <form action="/" method="get">
                    </form>
                    <p>Request an allocation from your inventory by submitting one at a time</p>
                    <table border="1">
                    <tr>
                    <th>name</th><th>capacity</th><th>goal</th><th>availability</th><th>advertiser ID</th><th>allocate</th>
                    </tr>
                    <%
                    if (alloc_Amount > 0 && set_name.length() > 0)
                    {
                        if (invState.GetItems(set_name, advertiserID, alloc_Amount))
                        	log.info(customer_name + " : Allocated " + set_name + " : " + Integer.toString(alloc_Amount));
                        else
                            log.severe(customer_name + " : Allocation failed for " + set_name + " of " + Integer.toString(alloc_Amount));
                    }
                    // build availabilities forms
                    // TODO: check inventory status first
                    Statement st = invState.getConnection().createStatement();
                    ResultSet rs = st.executeQuery("SELECT set_name, capacity, goal, availability FROM structured_data_base");
                    while (rs.next())
                    {
                        set_name = rs.getString(1);
                        pageContext.setAttribute("set_name", set_name);
                        int capacity = rs.getInt(2);
                        int goal = rs.getInt(3);
                        int availability = rs.getInt(4);
                        log.info(customer_name + " : " + set_name.toString() + ", " + Integer.toString(capacity) + ", " + Integer.toString(goal)  + ", " + Integer.toString(availability));
                        pageContext.setAttribute("availability", availability);
                        %>
                        <tr>
                        <td><%=set_name%></td>
                        <td><%=capacity%></td>
                        <td><%=goal%></td>
                        <td><%=availability%></td>
                        <form  action="/" method="post">
                        <input type="hidden" name="set_name" value="${fn:escapeXml(set_name)}"/>
                        <input type="hidden" name="customer_name" value="${fn:escapeXml(customer_name)}"/>
                        <input type="hidden" name="mode" value="<%=UIhelper.Mode.Allocate.toString()%>"/>
                        <td>
                        <input type="text" name="advertiserID" required/>
                        </td>
                        <td>
                        <input type="number" name="alloc_Amount" min="1" max="${fn:escapeXml(availability)}" required/>
                        </td>
                        <td>
                        <input type="submit" value="Submit"/>
                        </td>
                        </form>
                        </td>
                        </tr>
                        <%
                    }
                    invState.close();
                    %>
                    </table>
                    <br>        
                    <form action="/startsimulate" method="get">
                    <input type="hidden" name="customer_name" value="${fn:escapeXml(customer_name)}"/>
                    You can run the simulation: <input type="submit" value="Run"/>
                    </form>
                    <form action="/AllocationReport.jsp" method="get">
                    Show allocation report  <input type="submit" value="Report"/>
                    </form>       
                    <%
                    }
                	break;
                default:
                    // registered user, let her chose/create inventory
                    InventoryState invState2 = null;
                    try 
                    {
                    	invState2 = new InventoryState(customer_name, true);
                    }
                    catch (com.mysql.jdbc.exceptions.jdbc4.MySQLSyntaxErrorException ex)
                    {
                        log.severe(customer_name + " : exception when connecting to db: " + ex.getMessage());
                    }
                    catch (Exception ex)
                    {
                    	log.severe(customer_name + " : exception " + ex.getMessage());
                    }
                    if (invState2 == null)
                    {
                        %>
                        <p><font color="red">Data is corrupted. Contact <a href="mailto:admin@butterflywing.net">Support</a></font></p>
                        <%
                        return;
                    }
                    log.warning(customer_name + " : The inventory is " + invState2.getStatus());
                    if (invState2.isLoaded())
                    {
                     if (invState2.hasData())
                     {
                        %>
                        <form action="/" method="post">
                        <input type="hidden" name="mode" value="<%=UIhelper.Mode.Allocate.toString()%>"/>
                        Your can allocate impressions from <%=customer_name%> inventory <input type="submit" value="Allocate"/>
                        </form>
                        <p>Or you can start over and re-initialize your inventory with new data</p>
                        <%
                     }
                     else
                     {
                         %>
                         <p><font color="red">All availabilities in your data equal 0</font></p>
                         <p>You can start over and re-initialize your inventory with new data</p>
                         <%
                     }
                    }
                    else if (invState2.isSomethngWrong())
                    {
                    	switch (Status.valueOf(invState2.getStatus()))
                    	{
                        case wrongfile:
                            %>
                            <p><font color="red">Uploaded inventory file has wrong format. Upload corrected file and re-initialize the inventory</font></p>
                            <%
                            break;
                        case highoverlap:
                            %>
                            <p><font color="red">Degree of segments overlap exceeds the limit. Upload corrected file and re-initialize the inventory</font></p>
                            <%
                            break;
                        case manysegmens:
                            %>
                            <p><font color="red">Too many segments were specified. Upload corrected file and re-initialize the inventory</font></p>
                            <%
                            break;
                        case nosegments:
                            %>
                            <p><font color="red">No segments were specified. Upload corrected file and re-initialize the inventory</font></p>
                            <%
                            break;
                        case toomuchdata:
                            %>
                            <p><font color="red">Uploaded inventory file is over size limit. Upload corrected file and re-initialize the inventory</font></p>
                            <%
                            break;
                        case nodata:
                            %>
                            <p><font color="red">Uploaded inventory file does not have data matching segments. Upload correct file and re-initialize the inventory</font></p>
                            <%
                            break;
                        case unknown:
                            %>
                            <p><font color="red">Unknown error. Contact <a href="mailto:admin@butterflywing.net">Support</a></font></p>
                            <%
                            break;
                        default:
                            %>
                            <p><font color="red">Unknown error. Contact <a href="mailto:admin@butterflywing.net">Support</a></font></p>
                            <%
                            break;
                    	}
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
                    if (invFile.isLoaded() && !invState2.isSomethngWrong()) 
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
                    <input type="hidden" name="mode" value="<%=UIhelper.Mode.createAccount.toString()%>"/>
                    Create a secondary user account for your organization <input type="submit" value="Create account"/>
                    </form>
                    <%
                    }
                    invState2.close();
                break;
            }
        }
        if (modeStr != null)
        {
            %>
            <form action="/" method="post">
            Return to start page <input type="submit" value="Return"/>
            </form>
            <%
        }
        %>
            <p>Or you can <a href="<%= userService.createLogoutURL(request.getRequestURI()) %>">sign out</a></p>
        <%
    } 
    else 
    {
	    %>
	    <p>AdAllocator is the next-generation engine for booking online advertising campaigns.</p>
	    <p>It solves the major challenge posed before businesses buying and selling targeted online media, which is determining advertising opportunities available for guaranteed booking, while taking into account all the bookings already made.</p>
	    <p>To find out more read White Paper and <a href="<%= userService.createLoginURL(request.getRequestURI()) %>">Sign in</a> with your Google email.</p>
	    <%
    }
    %>
    <p><a href="/BookAdvertisingCampaignsInstantly.pdf" target="_blank">Read White Paper</a></p>
    <p><a href="/TestInventory.json" target="_blank">Download sample inventory file</a></p>
	<!-- Don't show it for now <p><a href="/EUA.pdf" target="_blank">Read Terms of Service</a></p> -->
</body>
</html>