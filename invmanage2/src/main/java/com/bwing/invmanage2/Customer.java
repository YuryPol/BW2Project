package com.bwing.invmanage2;

import com.googlecode.objectify.annotation.Entity;
import com.googlecode.objectify.annotation.Id;
import java.util.Date;
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
}
