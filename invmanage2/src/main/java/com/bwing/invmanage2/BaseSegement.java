package com.bwing.invmanage2;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.CharBuffer;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map.Entry;

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
	private int weight = 1;
	private BitSet key;

	public static boolean key_contains(BitSet superset, BitSet subset)
	{
		// super set contains sub set
		BitSet tmp = (BitSet) superset.clone();
		tmp.or(subset);
		return (tmp.equals(superset));
	}
	
	public static int getBitsCounts(HashMap<BitSet, BaseSegement> bss, double bcnts[], BufferedWriter bwTXT) throws IOException
	{
		CharBuffer cb = CharBuffer.allocate(bcnts.length);
		char[] filler = new char[bcnts.length];
		if (InventoryState.DEBUG && bwTXT != null) {
			Arrays.fill(filler, ' ');
		}
		
		BitSet accumulative = new BitSet();
		for (Entry<BitSet, BaseSegement> bs : bss.entrySet()) {
			accumulative.or(bs.getKey());
			
			if (InventoryState.DEBUG && bwTXT != null) {
				// clear the buffer
				cb.rewind();
				cb.put(filler);
//				cb.put(bcnts.length, '\n');
			}

			for (int index = 0; index >= 0; index++) {
				index = bs.getKey().nextSetBit(index);
				if (index < 0)
					break;
				if (InventoryState.DEBUG && bwTXT != null)
					cb.put(index, '*');
				++bcnts[index] ;
			}
			if (InventoryState.DEBUG && bwTXT != null) {
				cb.rewind();
				bwTXT.write(cb.toString() + "|" + bs.getValue().getcapacity() + "\n");
			}
		}
		return accumulative.cardinality();
	}

	BaseSegement() {
		this_criteria = new criteria();
		key = new BitSet();
	}
	
	public void setkeybit(int index) {
		key.set(index);
	}
	
	public void setkey(BitSet key) {
		this.key = key;
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
	
	public void setweight(int minCapacity) {
		weight = capacity / minCapacity;
	};
	public int getweight() {
		return weight;
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
