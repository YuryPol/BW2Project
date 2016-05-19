package com.bwing.invmanage2;

import com.googlecode.objectify.annotation.Entity;
import com.googlecode.objectify.annotation.Id;

import java.sql.SQLException;
import java.util.Date;
import java.util.List;

import com.google.appengine.api.users.User;
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
  private static final Logger log = Logger.getLogger(Customer.class.getName());
   
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
	  company = company_name;
	  founder = user;
  }
  
  static public Customer findCustomer(List<Customer> customers, String theCompany)
  {
	  if (customers.isEmpty())
	  {
		  System.out.println("No Customers exist");
		  return null;
	  }
	  
	  for (Customer icustomer : customers)
	  {
		  if (icustomer.company.toString().equals(theCompany))
		  {
			  log.warning("Customer already exists: " + theCompany);
			  return icustomer;
		  }
	  }
	  log.warning("No such Customer exists: " + theCompany);
	  return null;
  }

}
