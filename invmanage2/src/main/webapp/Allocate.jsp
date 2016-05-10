
<%@ page import="java.sql.ResultSet" %>
<%@ page import="java.sql.Statement" %>
<%@ page import="com.bwing.invmanage2.InventoryUser" %>
<%@ page import="com.bwing.invmanage2.Customer" %>
<%@ page import="com.bwing.invmanage2.InventoryState" %>
<%@ page import="com.googlecode.objectify.ObjectifyService" %>
<%@ page import="com.googlecode.objectify.Key" %>
<%@ page import="com.google.appengine.api.blobstore.BlobstoreServiceFactory" %>
<%@ page import="com.google.appengine.api.blobstore.BlobstoreService" %>
<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<%@ taglib prefix="fn" uri="http://java.sun.com/jsp/jstl/functions" %>

<html>
<head>
    <link type="text/css" rel="stylesheet" href="/stylesheets/main.css"/>
</head>
<body>
<%
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
        String customer_name = iuser.theCustomer.get().company;
        System.out.println("Customer: " + customer_name);
        pageContext.setAttribute("customer_name", customer_name);
        %>
        <p>You can return to starting page,</p>
        <form action="/" method="get"></form>
        <div><input type="submit" value="Return"/></div>
        <p>Or work with your inventory</p>
        <table border="1">
		<tr>
		<th>name</th><th>capacity</th><th>goal</th><th>availability</th><th>allocate</th><th>Submit</th>
		</tr>
        <%
        InventoryState invState = new InventoryState(customer_name);
        Statement st = invState.getConnection().createStatement();
        st.execute("USE " + InventoryState.BWdb + customer_name);
        // build availabilities forms
        ResultSet rs = st.executeQuery("SELECT set_name, capacity, goal, availability FROM structured_data_base");
        while (rs.next())
        {
        	%>
			<tr>
			<td><%=
			rs.getString(1)
			%></td>
            <td><%=
            rs.getInt(2)
            %></td>
            <td><%=
            rs.getInt(3)
            %></td>
            <td><%=
            rs.getInt(4)
            %></td>
			<td>
				<form action="/" method="get">
				<input type="text" name="alloc"/>
				<input type="submit" value="Submit"/>
				</form>
			</td>
			</tr>
        	<%
        }
        invState.close();
        %>
        </table>
        <%
     }
    %>

</body>
</html>