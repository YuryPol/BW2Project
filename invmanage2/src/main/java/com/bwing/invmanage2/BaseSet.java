package com.bwing.invmanage2;

import java.sql.Blob;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Set;

//    {
//      "name": "highrollers",
//      "creteria": {
//        "state": [
//          "NY",
//          "NJ"
//        ],
//        "income": [
//          "affluent",
//          "middle"
//        ],
//        "gender": [
//          "M"
//        ],
//        "content": [
//          "business",
//          "sport",
//          "food",
//          "news"
//        ],
//        "age": [
//          "middle",
//          "young",
//          "child"
//        ]
//      },
//    }
    
public class BaseSet {
	private String name=null;
	private criteria this_criteria;
	private int availablity = 0;
	private int goal=0;
	private BitSet key;
	
	BaseSet() {
		this_criteria = new criteria();
		key = new BitSet();
	}
	
	public BaseSet(int i) {
		this_criteria = new criteria();
		key = new BitSet(i);
	}

	public String getname() {
		return name;
	};
	public void setname(String nm) {
		name = nm;
	};
	public void setkey(int index) {
		key.set(index);
	}
	public BitSet getkey() {
		return key;
	}
	
	public criteria getCriteria() {
		return this_criteria;
	};
	public void setCriteria(criteria crt) {
		this_criteria = crt;
	};
	
	public int getgoal() {
		return goal;
	};
	public void setgoal(int gl) {
		goal = gl;
	};
	
	public boolean contains(criteria another) {
		// get this names
		Set<String> thisNames = this_criteria.keySet();
		Set<String> anotherNames = another.keySet();
		if (anotherNames.containsAll(thisNames))
		{
			// another criteria contains all names of this one, so it is more (or the same) specific
		    // check elements
		    for (String name: thisNames) {
		    	HashSet<String> anotherValues = another.get(name);
		    	HashSet<String> thisValues = this_criteria.get(name);
		    	if (!thisValues.containsAll(anotherValues))
		    		// this criteria does not contain all another's criteria values so it is more specific
		    		return false;
		    }
			return true;
		}
		else
			// it defies the common sense as each criterion AND-ed with others 
			// fewer selection criteria means wider set
			return false;
	}

	boolean contains(BaseSet another)
	{
		BitSet tmp = key;
		tmp.or(another.key);
		return (tmp == key);
	}
	
	boolean modifyAvailablity(int change)
	{
		if (availablity + change >= 0)
		{
			availablity += change;
			return true;
		}
		else return false;
	}
	
	void unionWith(BaseSet another)
	{
		key.or(another.key);
	}

	public Blob getKeyBlob(Connection con) throws SQLException 
	{
	    byte[] byteArray = key.toByteArray();	    
	    Blob blob = con.createBlob(); //con is your database connection created with DriverManager.getConnection();	    
	    blob.setBytes(1, byteArray);	 
	    
	    return blob;
	}
	 
	public BitSet setKeyBlob(Blob blob) throws SQLException {
	    byte[] bytes = blob.getBytes(1, (int)blob.length());
	    BitSet bitSet = BitSet.valueOf(bytes);
	 
	    return bitSet;
	}
	 
	public long[] getKeyBin()
	{
	    return key.toLongArray();	    
	}
	 
	public BitSet setKeyVarBin(long[] words) {
	    return BitSet.valueOf(words);
	}

}
