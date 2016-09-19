package invmanage2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;

import com.bwing.invmanage2.InventroryData;
import com.bwing.invmanage2.criteria;
import com.bwing.invmanage2.inventoryset;
import com.bwing.invmanage2.segment;

public class GenerateInventory 
{	
	private static final String[] Regions = new String[] { "Northeast", "Midwest", "South", "West" };
	private static final String[] Interests = new String[] 
			{"sport", "business", "cooking", "movies", "politics", "nature", "cars", "food", "investments", "golf", "vine", "football", "sport", "tech"};
	private static final String[] Genders = new String[] {"F", "M"};
	private static final String[] Ages = new String[] {"child", "young", "middle", "senior"};
	private static final String[] Incomes = new String[] {"poor", "middle", "affluent", "rich"};
	private static final String[] Contents = new String[] {"drama", "comedy", "news", "actions", "travel"};
	
	//public static final HashSet<String> StatesSet = new HashSet<String>(Arrays.asList(States));
	
	private HashMap<String, HashSet<String>> criterion; 

	public static void main(String[] args) {
		int inventorysets_count = Integer.parseInt(args[0]);
		int max_inventoryset_count = Integer.parseInt(args[1]);
		int segments_count = Integer.parseInt(args[2]);
		int max_segment_count = Integer.parseInt(args[3]);
        Random rand = new Random();
		HashSet<String> some_set = new HashSet<String>();
		
		// Generate inventory sets
		ArrayList<inventoryset> inventorysets = new ArrayList<inventoryset>();
		for (int ind = 0; ind < inventorysets_count; ind++)
		{
			inventoryset set = new inventoryset();
			criteria some_criteria = new criteria();
			set.setName("Audience_" + Integer.toString(ind, 10));

			if (rand.nextInt(10) > 1) // only 10% of inventory sets have require region
			{
				some_set.clear();
				some_set.add(Regions[rand.nextInt(Regions.length)]);
				some_criteria.putIfAbsent("region", some_set); 
				set.setcriteria(some_criteria);
			}

			if (rand.nextInt(3) > 1) // only 66% of inventory sets require interests
			{
				some_set.clear();
				while (true) {
					if (!some_set.add(Interests[rand.nextInt(Interests.length)]))
						break;
				}
				some_criteria.putIfAbsent("interests", some_set);
				set.setcriteria(some_criteria);
			}

			if (rand.nextInt(4) > 1) // only 25% of inventory sets require gender
			{
				some_set.clear();
				some_set.add(Genders[rand.nextInt(Genders.length)]);
				some_criteria.putIfAbsent("gender", some_set);
				set.setcriteria(some_criteria);
			}

			if (rand.nextInt(5) > 1) // only 20% of inventory sets require age
			{
				some_set.clear();
				some_set.add(Ages[rand.nextInt(Ages.length)]);
				some_criteria.putIfAbsent("age", some_set);
				set.setcriteria(some_criteria);
			}

			if (rand.nextInt(10) > 1) // only 10% of users have income known
			{
				some_set.clear();
				some_set.add(Incomes[rand.nextInt(Incomes.length)]);
				some_criteria.putIfAbsent("income", some_set);
				set.setcriteria(some_criteria);
			}

			if (rand.nextInt(2) > 1) // only 50% of paves have content defined
			{
				some_set.clear();
				some_set.add(Contents[rand.nextInt(Contents.length)]);
				some_criteria.putIfAbsent("content", some_set);
				set.setcriteria(some_criteria);
			}			
		}
		
		// Generate segments
		ArrayList<segment> segments = new ArrayList<segment>();
		for (int ind = 0; ind < segments_count; ind++)
		{
			segment seg = new segment();
			seg.setCount(rand.nextInt(max_segment_count));
			criteria some_criteria = new criteria();

			some_set.clear();
			some_set.add(Regions[rand.nextInt(Regions.length)]);
			some_criteria.putIfAbsent("region", some_set); // all users have
															// region
			seg.setcriteria(some_criteria);

			if (rand.nextInt(3) > 1) // only 66% of users have interests known
			{
				some_set.clear();
				while (true) {
					if (!some_set.add(Interests[rand.nextInt(Interests.length)]))
						break;
				}
				some_criteria.putIfAbsent("interests", some_set);
				seg.setcriteria(some_criteria);
			}

			if (rand.nextInt(2) > 1) // only 50% of users have gender known
			{
				some_set.clear();
				some_set.add(Genders[rand.nextInt(Genders.length)]);
				some_criteria.putIfAbsent("gender", some_set);
				seg.setcriteria(some_criteria);
			}

			if (rand.nextInt(5) > 1) // only 20% of users have age known
			{
				some_set.clear();
				some_set.add(Ages[rand.nextInt(Ages.length)]);
				some_criteria.putIfAbsent("age", some_set);
				seg.setcriteria(some_criteria);
			}

			if (rand.nextInt(10) > 1) // only 10% of users have income known
			{
				some_set.clear();
				some_set.add(Incomes[rand.nextInt(Incomes.length)]);
				some_criteria.putIfAbsent("income", some_set);
				seg.setcriteria(some_criteria);
			}

			if (rand.nextInt(2) > 1) // only 50% of pages have content defined
			{
				some_set.clear();
				some_set.add(Contents[rand.nextInt(Contents.length)]);
				some_criteria.putIfAbsent("content", some_set);
				seg.setcriteria(some_criteria);
			}			
		}
		
		InventroryData inventorydata = new InventroryData();
		inventorydata.setSegments(segments.toArray(new segment[segments.size()]));
		inventorydata.setInventorysets(new inventoryset[inventorysets.size()]);
	}

}
