<%@ page import="com.bwing.invmanage2.InventoryUser" %>
<%@ page import="com.bwing.invmanage2.InventoryState" %>
<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>
<%@ page import="java.util.logging.Logger" %>
<%@ page import="com.google.appengine.api.taskqueue.Queue"%>
<%@ page import="com.google.appengine.api.taskqueue.QueueFactory"%>
<%@ page import="com.google.appengine.api.taskqueue.TaskOptions"%>
<%@ page import="java.sql.Connection"%>
<%@ page import="java.sql.PreparedStatement"%>
<%@ page import="java.sql.ResultSet"%>
<%@ page import="java.sql.DatabaseMetaData"%>

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
        if (message != null && message.equals("wasRunning")){
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
           Connection con = invState.getConnection();
           DatabaseMetaData dbm = con.getMetaData();
           // check if "result_serving" table is there
           ResultSet tables = dbm.getTables(null, null, "result_serving", null);
           if (tables.next()) {
	           PreparedStatement getCompletionPercnt = con.prepareStatement(
	                   "select round(max((goal-served_count)/goal)*100) from result_serving");
	           ResultSet rs = getCompletionPercnt.executeQuery();
	           int completionPercnt = 0;
	           if (rs.next())
	        	   completionPercnt = 100 - rs.getInt(1);
	           %>
	           Waiting for <%=customer_name%> simulation to complete
	           <p>So far <%=completionPercnt%>% completed</p>
	           <p id="demo"></p>   
	           <script>
	           document.getElementById("demo").innerHTML = Date();
	           </script>    
	           <%
	           invState.close();
	           response.setIntHeader("Refresh", 5);
	           log.info("Refreshing page waiting for inventory to load");
           }
           else {
               %>
               <p>Simulation was completed</p>
               <p>Return to start page</p>
               <form action="/" method="get">
                   <div>
                       <input type="submit" value="Return" />
                   </div>
               </form>
               <%
           }
        }
    }
    %>

</body>
</html>