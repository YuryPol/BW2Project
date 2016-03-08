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

import com.google.appengine.api.users.User;
import com.google.appengine.api.users.UserService;
import com.google.appengine.api.users.UserServiceFactory;

import com.googlecode.objectify.ObjectifyService;


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
	  @Parent public Ref<Customer> theCustomer;
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
	  
	  public InventoryUser(Ref<Customer> company, User theUser, String first_name, String last_name, PhoneNumber phone, String email)
	  {
		  this();
		  theCustomer = company;
		  user = theUser;
		  user_first_name = first_name;
		  user_last_name = last_name;
		  user_phone = phone;
		  user_email = email;
	  }
	  
/*	  static public boolean ContainedIn(List<InventoryUser> users, User user)
	  {
		  if (users.isEmpty())
		  {
			  System.out.println("No InventoryUsers exists");
			  return false;
		  }
		  
		  for (InventoryUser iuser : users)
		  {
			  if (iuser == null)
				  return false;
			  else if (iuser.user.getEmail().toString().equals(user.getEmail().toString()))
			  {
				  System.out.println("User already exists: " + user.getEmail().toString());
				  return true;
			  }
		  }
		  System.out.println("No such InventoryUser exists: " + user.getEmail().toString());
		  return false;
	  }
*/	  
	  static public InventoryUser findInventoryUser(List<InventoryUser> iuserrs, String email)
	  {
		  if (iuserrs == null || iuserrs.isEmpty())
		  {
			  System.out.println("No Users exist");
			  return null;
		  }
		  
		  for (InventoryUser iuser : iuserrs)
		  {
			  if (iuser.user_email.toString().equals(email))
			  {
				  System.out.println("User already exists: " + email);
				  return iuser;
			  }
		  }
		  System.out.println("No such User exists: " + email);
		  return null;
	  }
	  
	  static public InventoryUser getCurrentUser()
	  {
			UserService userService = UserServiceFactory.getUserService();
			User theUser = userService.getCurrentUser();
			if (theUser != null) {
				List<InventoryUser> users = ObjectifyService.ofy().load().type(InventoryUser.class) // We want only Users
						// .filter("user_email", theUser.getEmail())
						.list();
				InventoryUser iuser = InventoryUser.findInventoryUser(users, theUser.getEmail());
				return iuser;
			}
			return null;
	  }
}
