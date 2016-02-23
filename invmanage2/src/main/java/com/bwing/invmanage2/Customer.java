package com.bwing.invmanage2;

import com.googlecode.objectify.annotation.Entity;
import com.googlecode.objectify.annotation.Id;
import java.util.Date;

/**
 * The @Entity tells Objectify about our entity.  We also register it in {@link OfyHelper}
 *
 * NOTE - all the properties are PUBLIC so that can keep the code simple.
 **/
@Entity
public class Customer {
  @Id public String customer;
  public InventoryUser founder;
  public Date date;

  /**
   * Simple constructor just sets the date and the entity founder
   **/
/*  public Customer(InventoryUser user) {
    date = new Date();
    founder = user;
  }
*/

}
