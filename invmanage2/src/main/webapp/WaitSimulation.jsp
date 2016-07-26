<%@ page import="com.bwing.invmanage2.InventoryUser" %>
<%@ page import="com.bwing.invmanage2.InventoryState" %>
<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>
<%@ page import="java.util.logging.Logger" %>
<%@ page import="com.google.appengine.api.taskqueue.Queue"%>
<%@ page import="com.google.appengine.api.taskqueue.QueueFactory"%>
<%@ page import="com.google.appengine.api.taskqueue.TaskOptions"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1">
<title>Insert title here</title>
</head>
<body>
    <%
    Logger log = Logger.getLogger(this.getClass().getName());
    log.info("Now waiting for simulation to complete");
    // check the user
    InventoryUser iuser = InventoryUser.getCurrentUser();
    if (iuser == null)
    {
    log.warning("user was logged out");
    %>
    <p>ERROR! You were logged out</p>
    <p>Return to start page</p>
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
        String message = request.getParameter("message");
        	%>
        	<p>Simulation was already running</p>
		    <p>Return to start page</p>
		    <form action="/" method="get">
		        <div>
		            <input type="submit" value="Return" />
		        </div>
		    </form>
        	<%
        }
        else if (invState.isLocked()) 
        {
            %>
            <p>Inventory is locked</p>
		    <p>Return to start page</p>
		    <form action="/" method="get">
		        <div>
		            <input type="submit" value="Return" />
		        </div>
		    </form>
		    <%
        }
        else
        {
           Queue queue = QueueFactory.getDefaultQueue();
           queue.add(TaskOptions.Builder.withUrl("/simulate").param("customer_name", customer_name));
           log.info(customer_name + " simulaton added to default queue.");
           %>
           Waiting for <%=customer_name%> simulation to complete
           <p id="demo"></p>   
           <script>
           document.getElementById("demo").innerHTML = Date();
           </script>    
           <%
           invState.close();
           response.setIntHeader("Refresh", 5);
           log.info("Refreshing page waiting for inventory to load");
        }
    }
    %>

</body>
</html>
