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
<%@ page import="java.util.Date" %>
<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<%@ taglib prefix="fn" uri="http://java.sun.com/jsp/jstl/functions" %>

<html>
<head>
    <link type="text/css" rel="stylesheet" href="/stylesheets/main.css"/>
    <title>Allocation report</title>
</head>
<body>
<%
    Logger log = Logger.getLogger(this.getClass().getName());
    // check the user
    InventoryUser iuser = InventoryUser.getCurrentUser();
    if (iuser == null)
    {
        %>
        <p>ERROR! You were logged out. Return to login page: 
        <form action="/" method="get">
        <div><input type="submit" value="Return"/></div>
        </form>
        </p>
        <%
    }
    else 
    {
        String customer_name = iuser.theCustomer.get().company;
        InventoryState invState = new InventoryState(customer_name);
        Date date = new Date();
        %>
        Report for <%=customer_name%> inventory allocations as of <%=date.toString() %>
        <table border="1">
        <tr>
        <th>name</th><th>capacity</th><th>availability</th><th>advertiser ID</th><th>goal</th>
        <%
        Statement st = invState.getConnection().createStatement();
        st.execute("USE " + InventoryState.BWdb + customer_name);
        // TODO: check inventory status first
        ResultSet rs = st.executeQuery("SELECT set_name, capacity, availability, advertiserID, goal FROM allocation_protocol");
        while (rs.next())
        {
            String set_name = rs.getString(1);
            int capacity = rs.getInt(2);
            int availability = rs.getInt(3);
            String advertiserID = rs.getString(4);
            int goal = rs.getInt(5);
            %>
            <tr>
            <td><%=set_name%></td>
            <td><%=capacity%></td>
             <td><%=availability%></td>
            <td><%=advertiserID%>
            <td><%=goal%></td>
            </tr>
            <%
        }
        invState.close();
        %>
        </table>
        <br>        
        <form action="/" method="get">
        You can return to starting page: <input type="submit" value="Return"/>
        </form>
        <%
     }
    %>

</body>
</html>