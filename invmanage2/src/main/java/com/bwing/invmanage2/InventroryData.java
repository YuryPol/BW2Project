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
  "inventorysets": [
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
  "segments": [
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
	  
	  private inventoryset[] inventorysets;
	  private segment[] segments;

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
	 * @return the inventorysets
	 */
	public inventoryset[] getInventorysets() {
		return inventorysets;
	}

	/**
	 * @param inventorysets the inventorysets to set
	 */
	public void setInventorysets(inventoryset[] inventorysets) {
		this.inventorysets = inventorysets;
	}

	/**
	 * @return the segments
	 */
	public segment[] getSegments() {
		return segments;
	}

	/**
	 * @param segments the segments to set
	 */
	public void setSegments(segment[] segments) {
		this.segments = segments;
	}

}
