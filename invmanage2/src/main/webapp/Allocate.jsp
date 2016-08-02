
<%@ page import="java.sql.ResultSet" %>
<%@ page import="java.sql.Statement" %>
<%@ page import="com.bwing.invmanage2.InventoryUser" %>
<%@ page import="com.bwing.invmanage2.Customer" %>
<%@ page import="com.bwing.invmanage2.InventoryState" %>
<%@ page import="com.googlecode.objectify.ObjectifyService" %>
<%@ page import="com.googlecode.objectify.Key" %>
<%@ page import="com.google.appengine.api.blobstore.BlobstoreServiceFactory" %>
<%@ page import="com.google.appengine.api.blobstore.BlobstoreService" %>
<%@ page import="java.util.logging.Logger" %>
<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<%@ taglib prefix="fn" uri="http://java.sun.com/jsp/jstl/functions" %>

<html>
<head>
    <link type="text/css" rel="stylesheet" href="/stylesheets/main.css"/>
</head>
<body>
<%
    Logger log = Logger.getLogger(this.getClass().getName());
    // check the user
    InventoryUser iuser = InventoryUser.getCurrentUser();
    if (iuser == null)
    {
        %>
        <p>ERROR! You were logged out</p>
        <p>Return to login page</p>
        <form action="/" method="get">
        <div><input type="submit" value="Return"/></div>
        </form>
        <%
    }
    else 
    {
        // registered user, let her chose/create inventory        
        String set_name = request.getParameter("set_name");
        String customer_name = iuser.theCustomer.get().company;
        int alloc_Amount = 0;
        String advertiserID = "";
        if (request.getParameter("alloc_Amount") != null)
        {
            alloc_Amount = Integer.parseInt(request.getParameter("alloc_Amount").trim());
            advertiserID = request.getParameter("advertiserID").trim();
        }
        // System.out.println("Customer: " + customer_name);
        pageContext.setAttribute("customer_name", customer_name);
        InventoryState invState = new InventoryState(customer_name);
        if (!invState.isLoaded())
        {
        	log.warning("The inventory " + customer_name + " is " + invState.getStatus().name());
        	%>
            <p>The inventory <%=customer_name%> is <%=invState.getStatus().name()%></p>
            <p>Return to start page</p>
            <form action="/" method="get">
            <div><input type="submit" value="Return"/></div>
            </form>
            <%
        }
        else {
        %>
        <p>You can return to starting page,</p>
        <form action="/" method="get">
        <div><input type="submit" value="Return"/></div>
        </form>
        <p>Or work with your inventory</p>
        <table border="1">
		<tr>
		<th>name</th><th>capacity</th><th>goal</th><th>availability</th><th>advertiser ID</th><th>allocate</th>
		</tr>
        <%
        Statement st = invState.getConnection().createStatement();
        st.execute("USE " + InventoryState.BWdb + customer_name);
        if (alloc_Amount > 0 && set_name.length() > 0)
        {
            invState.GetItems(set_name, advertiserID, alloc_Amount);
            log.info(set_name + " : " + Integer.toString(alloc_Amount));
        }
        // build availabilities forms
        // TODO: check inventory status first
        ResultSet rs = st.executeQuery("SELECT set_name, capacity, goal, availability FROM structured_data_base");
        while (rs.next())
        {
            set_name = rs.getString(1);
            pageContext.setAttribute("set_name", set_name);
            int capacity = rs.getInt(2);
            int goal = rs.getInt(3);
            int availability = rs.getInt(4);
            log.info(set_name.toString() + ", " + Integer.toString(capacity) + ", " + Integer.toString(goal)  + ", " + Integer.toString(availability));
            pageContext.setAttribute("availability", availability);
        	%>
			<tr>
			<td><%=set_name%></td>
            <td><%=capacity%></td>
            <td><%=goal%></td>
            <td><%=availability%></td>
			<form>
            <input type="hidden" name="set_name" value="${fn:escapeXml(set_name)}"/>
            <input type="hidden" name="customer_name" value="${fn:escapeXml(customer_name)}"/>
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
        // response.setIntHeader("Refresh",30);
        %>
        </table>
        <p>
        Or run the simulation
        <form action="/startsimulate" method="get">
        <input type="hidden" name="customer_name" value="${fn:escapeXml(customer_name)}"/>
        <div><input type="submit" value="Run Simulation"/></div>
        </form>
        </p>
        <%
     }
    }
    %>

</body>
</html>