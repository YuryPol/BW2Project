<%@ page import="com.bwing.invmanage2.InventoryUser" %>
<%@ page import="com.bwing.invmanage2.InventoryState" %>
<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
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
		<div>
			<input type="submit" value="Return" />
		</div>
	</form>
	<%
    }
    else 
    {
        String customer_name = iuser.theCustomer.get().company;
        InventoryState invState = new InventoryState(customer_name);
        if (invState.isLoaded())
        {
        	// Return to Select Inventory page
        	invState.close();
            response.sendRedirect("/SelectInventory.jsp");
        }
        else if (!invState.isValid())
        {
        	invState.close();
            %>
            <p>ERROR! <%=customer_name%> inventory failed to load</p>
            <p>Return to login page</p>
            <form action="/" method="get">
                <div>
                    <input type="submit" value="Return" />
                </div>
            </form>
            <%
        }
        else {
	        //pageContext.setAttribute("customer_name", customer_name);
		    %>
			Waiting for	<%=customer_name%> inventory to load
			<%
			invState.close();
			response.setIntHeader("Refresh", 5);
        }
    }
    %>
</body>
</html>