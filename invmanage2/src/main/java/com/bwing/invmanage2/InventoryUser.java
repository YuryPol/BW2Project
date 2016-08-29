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
import java.sql.SQLException;
import java.util.Date;
import java.util.List;
import java.util.logging.Logger;

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
	  @Id public String id;

	  public String user_first_name;
	  public String user_last_name;
	  public PhoneNumber user_phone;
	  public String bisness_email;
	  public boolean isPrimary;
	  @Index public Date date;
	  
	  private static final Logger log = Logger.getLogger(InventoryUser.class.getName());

	  public InventoryUser()
	  {
		  date = new Date();
	  }
	  
	  public InventoryUser(Ref<Customer> company, Email email, String first_name, String last_name, PhoneNumber phone, String bis_email, boolean isprimary) throws ClassNotFoundException, SQLException
	  {
		  this();
		  theCustomer = company;
		  id = email.getEmail();
		  user_first_name = first_name;
		  user_last_name = last_name;
		  user_phone = phone;
		  bisness_email = bis_email;
		  isPrimary = isprimary;
		  if (isprimary)
		  {
			  InventoryState.init(theCustomer.get().company);
		  }
	  }
	  
	  static public InventoryUser findInventoryUser(List<InventoryUser> iusers, String email)
	  {
		  if (iusers == null || iusers.isEmpty())
		  {
			  log.severe("No Users exist");
			  return null;
		  }
		  
		  for (InventoryUser iuser : iusers)
		  {
			  if (iuser.id.equals(email))
			  {
				  log.info("User found: " + email);
				  return iuser;
			  }
		  }
		  log.warning("No such User exists: " + email);
		  return null;
	  }
	  
	  static public InventoryUser findInventoryUser(String email)
	  {
			List<InventoryUser> users = ObjectifyService.ofy().load().type(InventoryUser.class) // We want only Users
			// .filter("user_email", theUser.getEmail())
			.list();
			return InventoryUser.findInventoryUser(users, email);
	  }
	  
	  
	  static public InventoryUser getCurrentUser(String company)
	  {
			UserService userService = UserServiceFactory.getUserService();
			User theUser = userService.getCurrentUser();
			if (theUser != null) {
				return ObjectifyService.ofy().load().type(InventoryUser.class).parent(Customer.getCustomer(company)).id(theUser.getEmail().toString()).now();				
			}
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
				return InventoryUser.findInventoryUser(users, theUser.getEmail());
			}
			return null;
	  }
	  
	  static public InventoryUser getGmailUser(String gmail)
	  {
		  // There is a presumption that there is only one user with gmail.
			List<InventoryUser> users = ObjectifyService.ofy().load().type(InventoryUser.class) // We want only Users
			// .filter("user_email", theUser.getEmail())
			.list();
			return InventoryUser.findInventoryUser(users, gmail);
	  }
	  
	  public Key<Customer> getCustomerKey()
	  {
		  return theCustomer.getKey();
	  }
}
