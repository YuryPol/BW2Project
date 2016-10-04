package com.bwing.invmanage2;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * 
 */

/**
 * @author "Yury"
 *
 */
public class criteria extends HashMap<String, HashSet<String>> implements Serializable 
{

	/**
	 * 
	 */
	private static final long serialVersionUID = -2146129670317599931L;

	/**
	 * 
	 */
	public criteria() {
		// TODO Auto-generated constructor stub
	}

	criteria(segment is)
	{
		putAll(is.getcriteria());
	}

/*	public boolean matches(criteria another) {
		if (another == null)
			return false;
		
		// get this names
		Set<String> thisNames = keySet();
		Set<String> anotherNames = another.keySet();
		if (anotherNames.containsAll(thisNames))
		{
			// another Criteria contains all names of this one, so it is more (or the same) specific
		    // check elements
		    for (String name: thisNames) {
		    	HashSet<String> anotherValues = another.get(name);
		    	HashSet<String> thisValues = get(name);
		    	if (Collections.disjoint(thisValues, anotherValues))
		    		// criteron's values are OR-ed with each other
		    		return false;
		    }
			return true;
		}
		else
			// it defies the common sense as each criterion AND-ed with others 
			// fewer selection criteria means wider set
			return false;
	}*/

	public static boolean matches(criteria one, criteria another) {
		if (another == null && one == null)
			return true;
		else if ((another == null && one != null) 
				|| (another != null && one == null))
			return false;
		
		// get this names
		Set<String> thisNames = one.keySet();
		Set<String> anotherNames = another.keySet();
		if (anotherNames.containsAll(thisNames))
		{
			// another Criteria contains all names of this one, so it is more (or the same) specific
		    // check elements
		    for (String name: thisNames) {
		    	HashSet<String> anotherValues = another.get(name);
		    	HashSet<String> oneValues = one.get(name);
		    	if (Collections.disjoint(oneValues, anotherValues))
		    		// criteron's values are OR-ed with each other
		    		return false;
		    }
			return true;
		}
		else
			// it defies the common sense as each criterion AND-ed with others 
			// fewer selection criteria means wider set
			return false;
	}
	
	public static boolean equals (criteria one, criteria another)
	{
		if (another == null && one == null)
			return true;
		else if ((another == null && one != null) 
				|| (another != null && one == null))
			return false;
		
		return one.equals(another);
	}
}
