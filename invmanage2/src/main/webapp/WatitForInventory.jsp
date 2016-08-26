<%@ page import="com.bwing.invmanage2.InventoryUser" %>
<%@ page import="com.bwing.invmanage2.InventoryState" %>
<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>
<%@ page import="java.util.logging.Logger" %>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
    <link type="text/css" rel="stylesheet" href="/stylesheets/main.css"/>
</head>
<body>
	<%
    Logger log = Logger.getLogger(this.getClass().getName());
	log.info("Now waiting for inventory to load");
    // check the user
    InventoryUser iuser = InventoryUser.getCurrentUser();
    if (iuser == null)
    {
    log.warning("user was logged out");
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
        InventoryState invState = new InventoryState(customer_name, true);
             %>
            Waiting for <%=customer_name%> inventory to load
		    <p id="demo"></p>   
		    <script>
		    document.getElementById("demo").innerHTML = Date();
		    </script>    
		    <%
 	     if (!invState.isValid()) 
	     {
 	    	// waiting for inventory to unlock
	     }
         else if (invState.isLoaded() || invState.isWrongFile())
         {
             // Return to Select Inventory page
             invState.close();
             response.sendRedirect("/");
         }
         else 
         {
      	    %>
      	    <p>No data loaded yet</p>
      	    <p>Return to inventory page</p>
      	    <form action="/" method="get">
      	        <div>
      	            <input type="submit" value="Return" />
      	        </div>
      	    </form>
      	    <%
         }
	     invState.close();
         response.setIntHeader("Refresh", 5);
         log.info("Refreshing wait page");
    }
    %>
</body>
</html>