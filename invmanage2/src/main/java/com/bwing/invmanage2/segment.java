package com.bwing.invmanage2;

import java.io.Serializable;

/**
 * 
 */

/**
 * @author Yury
 *
 */
public class segment implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 2014952295388849523L;
	/**
	      {
	      "name": "highrollers",
	      "criteria": {
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
	 * 
	 */
	
	private String name;
	private criteria this_criteria;

	public segment() {
		// goal = 0;
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
	 * @return the criteria
	 */
	public criteria getCriteria() {
		return this_criteria;
	}

	/**
	 * @param that_criteria the criteria to set
	 */
	public void setcriteria(criteria that_criteria) {
		this.this_criteria = that_criteria;
	}
}
