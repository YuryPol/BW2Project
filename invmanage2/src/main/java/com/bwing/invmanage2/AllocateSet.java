package com.bwing.invmanage2;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class AllocateSet extends HttpServlet 
{
    @Override
    public void doGet( HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException 
    {
        String set_name = request.getParameter("set_name");
        String customer_name = request.getParameter("customer_name");
        int amount = Integer.parseInt(request.getParameter("alloc_Amount"));

		try (InventoryState invState = new InventoryState(customer_name)) {
			invState.GetItems(set_name, amount);
		}
		catch (Exception ex) 
        {
            throw new ServletException(ex);
        }    	
    }
}
