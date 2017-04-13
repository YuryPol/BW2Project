package com.bwing.invmanage2;

import java.sql.Blob;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.BitSet;

/**
 * 
 */

/**
 * @author "Yury"
 *
 */
public class BaseSegement {

	private criteria this_criteria;
	private int capacity = 0;
	private BitSet key;

	BaseSegement() {
		this_criteria = new criteria();
		key = new BitSet();
	}
	
	public BaseSegement(int i) {
		this_criteria = new criteria();
		key = new BitSet(i);
	}

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
	
	public int getcapacity() {
		return capacity;
	};
	public void setcapacity(int cp) {
		capacity = cp;
	};
	public void addcapacity(int cp) {
		capacity += cp;
	};
	
	boolean contains(BaseSegement another)
	{
		// the segment contains another
		return key_contains(key, another.getkey());
	}
	

	void unionWith(BaseSegement another)
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
	
	public static boolean key_contains(BitSet superset, BitSet subset)
	{
		// super set contains sub set
		BitSet tmp = (BitSet) superset.clone();
		tmp.or(subset);
		return (tmp.equals(superset));
	}

	public int compareTo(BaseSegement another) {
		// Compare capacities
		if (this.key.cardinality() > another.key.cardinality())
			return 1;
		else if (this.key.cardinality() < another.key.cardinality())
			return -1; 
		else 
			return 0;
	}
}
