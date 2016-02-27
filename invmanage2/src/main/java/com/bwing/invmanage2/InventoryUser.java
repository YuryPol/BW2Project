package com.bwing.invmanage2;

import com.googlecode.objectify.annotation.Entity;
import com.googlecode.objectify.annotation.Id;
import com.googlecode.objectify.annotation.Index;
import com.googlecode.objectify.annotation.Parent;
import com.google.appengine.api.datastore.Email;
import com.google.appengine.api.datastore.PhoneNumber;
import com.google.appengine.api.users.User;
import com.googlecode.objectify.Key;
import com.googlecode.objectify.Ref;


import java.lang.String;
import java.util.Date;
import java.util.List;

/**
 * The @Entity tells Objectify about our entity.  We also register it in {@link OfyHelper}
 * Our primary key @Id is set automatically by the Google Datastore for us.
 *
 * We add a @Parent to tell the object about its ancestor. We are doing this to support many
 * customers.  Objectify, unlike the AppEngine library requires that you specify the fields you
 * want to index using @Index.  Only indexing the fields you need can lead to substantial gains in
 * performance -- though if not indexing your data from the start will require indexing it later.
 *
 * NOTE - all the properties are PUBLIC so that can keep the code simple.
 **/
@Entity
public class InventoryUser {
	  @Parent Ref<Customer> theCustomer;
	  @Id public Long id;

	  public User user;
	  public String user_first_name;
	  public String user_last_name;
	  public PhoneNumber user_phone;
	  public String user_email;
	  @Index public Date date;
	  
	  public InventoryUser()
	  {
		  date = new Date();
	  }
	  
	  public InventoryUser(Ref<Customer> company, String first_name, String last_name, PhoneNumber phone, String email)
	  {
		  this();
		  theCustomer = company;
		  user_first_name = first_name;
		  user_last_name = last_name;
		  user_phone = phone;
		  user_email = email;
	  }
}
