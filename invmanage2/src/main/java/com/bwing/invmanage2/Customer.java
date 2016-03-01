package com.bwing.invmanage2;

import com.googlecode.objectify.annotation.Entity;
import com.googlecode.objectify.annotation.Id;
import java.util.Date;
import java.util.List;

import com.google.appengine.api.users.User;

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
  
  public Customer()
  {
	  date = new Date();
  }

  /**
   * Simple constructor just sets the date and the entity founder
   **/
  public Customer(String company_name, User user) 
  {
    this();
    company = company_name;
    founder = user;
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
