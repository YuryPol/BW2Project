package com.bwing.invmanage2;

import java.util.BitSet;

import org.apache.commons.math3.ml.clustering.DoublePoint;

public class DoublePointBS extends DoublePoint {
	BitSet key;
	private int capacity = 0;

	public DoublePointBS(int[] point, BitSet key, int capacity) {
		super(point);
		this.key = key;
		this.capacity = capacity;
	}

	int getcapacity() {
		return capacity;
	}
	
	BitSet getkey() {
		return key;
	}
}
