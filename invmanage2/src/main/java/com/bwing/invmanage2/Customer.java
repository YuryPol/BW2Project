package com.bwing.invmanage2;

import com.googlecode.objectify.annotation.Entity;
import com.googlecode.objectify.annotation.Id;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.List;

import com.google.appengine.api.users.User;
import com.google.appengine.api.utils.SystemProperty;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The @Entity tells Objectify about our entity.  We also register it in {@link OfyHelper}
 *
 * NOTE - all the properties are PUBLIC so that can keep the code simple.
 **/
@Entity
public class Customer {
  @Id public String company;
  public User founder;
  public Date date;
  public String account;
  
  public Customer()
  {
	  date = new Date();
  }

  /**
   * Simple constructor just sets the date and the entity founder
   **/
  public Customer(String company_name, User user) throws ClassNotFoundException, SQLException
  {
	  this();
	  String url;
	  company = company_name;
	  founder = user;
	  account = company_name + "@'%'";
	  Connection con = null;
	  Statement st = null;
//	  try
//	  {
//		  // Connect to DB
//		  if (SystemProperty.environment.value() == SystemProperty.Environment.Value.Production) 
//		  {
//			  // Load the class that provides the new "jdbc:google:mysql://"
//			  // prefix.
//			  Class.forName("com.mysql.jdbc.GoogleDriver");
//			  url = "jdbc:google:mysql://<your-project-id>:<your-instance-name>/<your-database-name>?user=root";
//		  } 
//		  else 
//		  {
//			  // Local MySQL instance to use during development.
//			  Class.forName("com.mysql.jdbc.Driver");
//			  url = "jdbc:mysql://localhost:3306/demo?user=root&password=IraAnna12";
//		  }
//		  con = DriverManager.getConnection(url);
//		  st = con.createStatement();
//		  st.executeUpdate("CREATE USER " + account + " IDENTIFIED BY " + account + "_pass WITH MAX_USER_CONNECTIONS 1");
//	  } 
//	  catch (SQLException ex) {
//		  Logger lgr = Logger.getLogger(Customer.class.getName());
//		  lgr.log(Level.SEVERE, ex.getMessage(), ex);
//
//	  } 
//	  finally 
//	  {
//		  try 
//		  {
//			  if (st != null) {
//				  st.close();
//			  }
//			  if (con != null) {
//				  con.close();
//			  }
//		  } catch (SQLException ex) {
//			  Logger lgr = Logger.getLogger(Customer.class.getName());
//			  lgr.log(Level.WARNING, ex.getMessage(), ex);
//			  throw ex;
//		  }
//	  }

  }
  
  static public Customer findCustomer(List<Customer> customers, String theCompany)
  {
	  if (customers.isEmpty())
	  {
		  System.out.println("No Customers already exist");
		  return null;
	  }
	  
	  for (Customer icustomer : customers)
	  {
		  if (icustomer.company.toString().equals(theCompany))
		  {
			  System.out.println("Customer already exists: " + theCompany);
			  return icustomer;
		  }
	  }
	  System.out.println("No such Customer exists: " + theCompany);
	  return null;
  }

}
