package com.bwing.invmanage2;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Logger;

import org.apache.commons.math3.ml.clustering.CentroidCluster;
import org.apache.commons.math3.ml.clustering.KMeansPlusPlusClusterer;
import org.apache.commons.math3.ml.distance.EuclideanDistance;
import org.apache.commons.math3.random.JDKRandomGenerator;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.ml.clustering.DoublePoint;

public class BuildClusters {
	private static RandomGenerator random;
    private static final Logger log = Logger.getLogger(BuildClusters.class.getName());

	public static ArrayList<HashMap<BitSet, BaseSegement>> getClusters(HashMap<BitSet, BaseSegement> base_segments, int K, int maxIterations) {
		random = new JDKRandomGenerator();
		random.setSeed(1746432956321l);
		
		// generate points
		List<DoublePointBS> points = new ArrayList<DoublePointBS>();
		for (Entry<BitSet, BaseSegement> bs : base_segments.entrySet()) {
			DoublePointBS dp = getDoublePoint(bs.getKey(), bs.getValue().getcapacity());
			int pointCount = bs.getValue().getweight();
			while (pointCount-- > 0) {
				points.add(dp);  // ads a point as many times as its weight
			}
		}
		log.info("Total points = " + points.size() + " in " + base_segments.size() + " base segments.");

		// generate clusters
		KMeansPlusPlusClusterer<DoublePointBS> clusterer = new KMeansPlusPlusClusterer<DoublePointBS>(K, maxIterations,
				new EuclideanDistance(), random);
		List<CentroidCluster<DoublePointBS>> clusters = clusterer.cluster(points);
		
		// generate base sets for the clusters 
		ArrayList<HashMap<BitSet, BaseSegement>> base_segmens_list = new ArrayList<HashMap<BitSet, BaseSegement>>();
		int clasterIndex = 0;
		for (CentroidCluster<DoublePointBS> cluster : clusters) {
			log.info("cluster # " + clasterIndex + " with " + cluster.getPoints().size() + " points\n");
			HashMap<BitSet, BaseSegement> base_segments_tmp = new HashMap<BitSet, BaseSegement>();
			for (DoublePointBS pt : cluster.getPoints()) {
				BaseSegement tmp = getBaseSegement(pt);
				base_segments_tmp.put(tmp.getkey(), tmp);
			}
			base_segmens_list.add(base_segments_tmp);
			clasterIndex++;
		}

		return base_segmens_list;
	}
	
	static DoublePointBS getDoublePoint(BitSet bs, int capacity) {
		int bps[] = new int[bs.size()];
		int index = 0;

		while (true) {
			index = bs.nextSetBit(index);
			if (index == -1)
				break; // no more set bits
			bps[index++] = 1;
		}
		DoublePointBS db = new DoublePointBS(bps, bs, capacity);
		return db;
	}
	
	static BaseSegement getBaseSegement(DoublePointBS db) {
		BaseSegement bs = new BaseSegement();
		bs.setkey(db.getkey());
		bs.setcapacity(db.getcapacity());
		return bs;
	}
}
