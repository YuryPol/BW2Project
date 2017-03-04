<%@ page import="com.bwing.invmanage2.InventoryUser" %>
<%@ page import="com.bwing.invmanage2.InventoryState" %>
<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>
<%@ page import="java.util.logging.Logger" %>
<%@ page import="java.util.logging.Level" %>
<%@ include file="PreventCache.jsp" %>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
    <link type="text/css" rel="stylesheet" href="/stylesheets/main.css"/>
</head>
<body>
	<%
    Logger log = Logger.getLogger(this.getClass().getName());
    log.setLevel(Level.INFO);
	// log.info("Now waiting for inventory to load");
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
         String message = request.getParameter("message");
         %>
         Waiting for <%=customer_name%> inventory to load
	     <p id="demo"></p>   
	     <script>
	     document.getElementById("demo").innerHTML = Date();
	     </script>    
	     <%
         // log.info("The inventory " + customer_name + " is " + invState.getStatus().name());
         if (message != null && message.equals("Cancel"))
         {
            // Remove tables, if any created, thus causing Load to crash
            invState.clear();
            invState.close();
            log.info("Canceling wait inventory page");
            response.sendRedirect("/");
            return;
         }
         else if (!(invState.isLoadInProgress() || invState.isLoadStarted()))
         {
             // Return to Select Inventory page
             log.info("Inventory is in " + invState.getStatus() + " state, leave wait page");
             invState.close();
             response.sendRedirect("/");
             return;
         }
         else 
         {
      	    %>
             <form action="/" method="get">
             Return to start page: <input type="submit" value="Return" />
             </form>
             <br>
             <form action="WatitForInventory.jsp" method="post">
             <input type="hidden" name="message" value="Cancel"/>
             To cancel inventory load and return to start page: <input type="submit" value="Cancel" />
             </form>
      	    <%
         }
	     invState.close();
         response.setIntHeader("Refresh", 5);
         // log.info("Refreshing wait page");
    }
    %>
</body>
</html>