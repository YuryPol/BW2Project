<%@ page import="com.bwing.invmanage2.InventoryUser" %>
<%@ page import="com.bwing.invmanage2.InventoryState" %>
<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>
<%@ page import="java.util.logging.Logger" %>
<%@ page import="java.util.logging.Level" %>
<%@ page import="com.google.appengine.api.taskqueue.Queue"%>
<%@ page import="com.google.appengine.api.taskqueue.QueueFactory"%>
<%@ page import="com.google.appengine.api.taskqueue.TaskOptions"%>
<%@ page import="java.sql.Connection"%>
<%@ page import="java.sql.Statement"%>
<%@ page import="java.sql.ResultSet"%>
<%@ page import="java.sql.DatabaseMetaData"%>
<%@ page import="java.util.Date" %>
<%@ taglib prefix="fn" uri="http://java.sun.com/jsp/jstl/functions" %>
<%@ include file="PreventCache.jsp" %>

<html>
<head>
    <link type="text/css" rel="stylesheet" href="/stylesheets/main.css"/>
    <title>Simulation run</title>
</head>
<body>
    <%
    Logger log = Logger.getLogger(this.getClass().getName());
    log.setLevel(Level.INFO);
    // log.info("Now waiting for simulation to complete");
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
        pageContext.setAttribute("customer_name", customer_name);
        InventoryState invState = new InventoryState(customer_name, true);
        String message = request.getParameter("message");
        if (message != null && message.equals("Cancel"))
        {
            // Remove result_serving table, thus causing Simulation to crash
            Connection con = invState.getConnection();
            Statement st = con.createStatement();
            st.executeUpdate("DROP TABLE IF EXISTS result_serving");
            response.sendRedirect("/");
            return;
        }
        else if (!invState.isLoaded()) 
        {
            %>
            <p>Inventory is not loaded</p>
            <form action="/" method="get">
            Return to start page<input type="submit" value="Return" />
            </form>
            <form action="/WaitSimulation.jsp" method="post">
            <input type="hidden" name="message" value="Cancel"/>
            To cancel simulation and return to start page: <input type="submit" value="Cancel" />
            </form>
            <%
        }
        else
        {
           Connection con = invState.getConnection();
           DatabaseMetaData dbm = con.getMetaData();
           // check if "result_serving" table is there
           ResultSet tables = dbm.getTables(null, null, "result_serving", null);
           Date date = new Date();
           Statement st = con.createStatement();
           if (tables.next()) {
               %>
               <p>Report for <%=customer_name%> inventory allocations on <%=date.toString() %></p>
               <p>Waiting for simulation to complete</p>
               <table border="1">
               <tr>
               <th>segment</th><th>availability</th><th>goal</th><th>served_count</th><th>percent served</th>
               <%
               try {
               ResultSet rs = st.executeQuery("select set_name, availability, goal, served_count, round(served_count/goal, 4)*100 from result_serving");
               String set_name = "";
               int availability = 0;
               int goal = 0;
               int served_count = 0;
               float percent_served = 0;
               int total_goal = 0;
               long total_served_count = 0;
	   	       while (rs.next())
	   	       {
	   	            set_name = rs.getString(1);
	   	            availability = rs.getInt(2);
	   	            goal = rs.getInt(3);
	   	            served_count = rs.getInt(4);
	   	            percent_served = rs.getFloat(5);
	   	            total_goal += goal;
	   	            total_served_count += served_count; 
	   	            %>
	   	            <tr>
	   	            <td><%=set_name%></td>
	   	            <td><%=availability%></td>
	   	            <td><%=goal%></td>
	   	            <td><%=served_count%></td>
	   	            <td><%=percent_served%></td>
	   	            </tr>
	   	            <%
	   	       }	
	   	       invState.close();
	           response.setIntHeader("Refresh", 5);
	           log.info("Inventory: " + customer_name + ". Totally served " + total_served_count + " impressions out of allocated " + total_goal + " impressions, or " 
	                   + Math.round((total_served_count*100.0)/total_goal));
	           %>
	           </table>
	           <p>Totally served <%=total_served_count%> impressions out of allocated <%=total_goal%> impressions, or <%=Math.round((total_served_count*100.0)/total_goal)%> %</p>
               <br>               
	           <form action="/" method="get">
	           Return to start page: <input type="submit" value="Return" />
	           </form>
               <form action="/WaitSimulation.jsp" method="post">
               <input type="hidden" name="message" value="Cancel"/>
               To cancel simulation and return to start page: <input type="submit" value="Cancel" />
               </form>
	           <%
               }
               catch (Exception ex)
               {
                   response.sendRedirect("/");
                   return;                 
               }
           }
           else {
               %>
               <p>Report for <%=customer_name%> inventory allocations on <%=date.toString() %></p>
               <p>Simulation was completed</p>
               <table border="1">
               <tr>
               <th>segment</th><th>availability</th><th>goal</th><th>served_count</th><th>percent served</th>
               <%
               try {
               ResultSet rs = st.executeQuery("select set_name, availability, goal, served_count, round(served_count/goal, 4)*100 from result_serving_copy");
               String set_name = "";
               int availability = 0;
               int goal = 0;
               int served_count = 0;
               float percent_served = 0;
               int total_goal = 0;
               long total_served_count = 0;
               while (rs.next())
               {
                    set_name = rs.getString(1);
                    availability = rs.getInt(2);
                    goal = rs.getInt(3);
                    served_count = rs.getInt(4);
                    percent_served = rs.getFloat(5);
                    total_goal += goal;
                    total_served_count += served_count; 
                    %>
                    <tr>
                    <td><%=set_name%></td>
                    <td><%=availability%></td>
                    <td><%=goal%></td>
                    <td><%=served_count%></td>
                    <td><%=percent_served%></td>
                    </tr>
                    <%
               }    
               invState.close();
               log.info("Inventory: " + customer_name + ". Totally served " + total_served_count + " impressions out of allocated " + total_goal + " impressions, or " 
               + Math.round((total_served_count*100.0)/total_goal));
               %>
               </table>
               <p>Totally served <%=total_served_count%> impressions out of allocated <%=total_goal%> impressions, or <%=Math.round((total_served_count*100.0)/total_goal)%>%</p>
               <br>               
               <form action="/" method="get">
               Return to start page: <input type="submit" value="Return" />
               </form>
               <%
               }
               catch (Exception ex)
               {
                   response.sendRedirect("/");
                   return;                 
               }               
           }
        }
    }
    %>

</body>
</html>