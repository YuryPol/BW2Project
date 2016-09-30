package com.bwing.invmanage2;

import java.io.Serializable;

/**
 * 
 */

/**
 * @author Yury
 *
 */
public class InventroryData implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1654333881795619159L;
	/**
  "version": 1,
  "owner": "me",
  "name": "choices for me",
  "update": false,
  "opportunities": [
    {
      "name": "highrollers",
      "creteria": {
        "state": [
          "NY",
          "NJ"
        ],
        "income": [
          "affluent",
          "middle"
        ],
        "gender": [
          "M"
        ],
        "content": [
          "business",
          "sport",
          "food",
          "news"
        ],
        "age": [
          "middle",
          "young",
          "child"
        ]
      },
      "goal": 150000
    }
  ],
  "opportunities": [
    {
      "creteria": {
        "state": [
          "CA"
        ],
        "income": [
          "middle"
        ],
        "gender": [
          "F"
        ],
        "content": [
          "sport",
          "food"
        ],
        "age": [
          "young"
        ]
      },
      "count": 80000
    },
    {
      "criteria": {},
      "count": 550000
    }
  ]
}	 */
	
	  private int version;
	  private String owner;
	  private String name;
	  private Boolean update;
	  
	  private segment[] segments;
	  private opportunity[] opportunities;

	public InventroryData() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @return the version
	 */
	public int getVersion() {
		return version;
	}

	/**
	 * @param version the version to set
	 */
	public void setVersion(int version) {
		this.version = version;
	}

	/**
	 * @return the owner
	 */
	public String getOwner() {
		return owner;
	}

	/**
	 * @param owner the owner to set
	 */
	public void setOwner(String owner) {
		this.owner = owner;
	}

	/**
	 * @return the name
	 */
	public String getName() {
		return name;
	}

	/**
	 * @param name the name to set
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * @return the update
	 */
	public Boolean getUpdate() {
		return update;
	}

	/**
	 * @param update the update to set
	 */
	public void setUpdate(Boolean update) {
		this.update = update;
	}

	/**
	 * @return the segments
	 */
	public segment[] getSegments() {
		return segments;
	}

	/**
	 * @param inventorysets the inventorysets to set
	 */
	public void setInventorysets(segment[] inventorysets) {
		this.segments = inventorysets;
	}

	/**
	 * @return the opportunities
	 */
	public opportunity[] getOpportunities() {
		return opportunities;
	}

	/**
	 * @param opportunities the opportunities to set
	 */
	public void setOpportunities(opportunity[] opportunities) {
		this.opportunities = opportunities;
	}

}
