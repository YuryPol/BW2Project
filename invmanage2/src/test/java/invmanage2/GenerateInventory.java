package invmanage2;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;

import com.bwing.invmanage2.InventroryData;
import com.bwing.invmanage2.criteria;
import com.bwing.invmanage2.inventoryset;
import com.bwing.invmanage2.segment;
import com.fasterxml.jackson.databind.ObjectMapper;

public class GenerateInventory 
{	
	private static final String[] Regions = new String[] { "Northeast", "Midwest", "South", "West" };
	private static final String[] Interests = new String[] 
			{"sport", "business", "cooking", "movies", "politics", "nature", "cars", "food", "investments", "golf", "vine", "football", "sport", "tech"};
	private static final String[] Genders = new String[] {"F", "M"};
	private static final String[] Ages = new String[] {"child", "young", "middle", "senior"};
	private static final String[] Incomes = new String[] {"poor", "middle", "affluent", "rich"};
	private static final String[] Contents = new String[] {"drama", "comedy", "news", "actions", "travel"};
	
    private static ObjectMapper mapper = new ObjectMapper();
	
	public static void main(String[] args) {
		int inventorysets_count = Integer.parseInt(args[0]);
		int segments_count = Integer.parseInt(args[1]);
		int max_segment_count = Integer.parseInt(args[2]);
        Random rand = new Random();
		
		// Generate inventory sets
		ArrayList<inventoryset> inventorysets = new ArrayList<inventoryset>();
		for (int ind = 0; ind < inventorysets_count; ind++)
		{
			inventoryset set = new inventoryset();
			criteria some_criteria = new criteria();
			set.setName("Audience_" + Integer.toString(ind, 10));

			if (rand.nextInt(10) > 1) // only 10% of inventory sets have require region
			{
				HashSet<String> criterion = new HashSet<String>();
				criterion.add(Regions[rand.nextInt(Regions.length)]);
				some_criteria.putIfAbsent("region", criterion); 
			}

			if (rand.nextInt(3) > 1) // only 66% of inventory sets require interests
			{
				HashSet<String> criterion = new HashSet<String>();
				while (true) 
				{
					if (!criterion.add(Interests[rand.nextInt(Interests.length)]))
						break;
				}
				some_criteria.putIfAbsent("interests", criterion);
			}

			if (rand.nextInt(4) > 1) // only 25% of inventory sets require gender
			{
				HashSet<String> criterion = new HashSet<String>();
				criterion.add(Genders[rand.nextInt(Genders.length)]);
				some_criteria.putIfAbsent("gender", criterion);
			}

			if (rand.nextInt(5) > 1) // only 20% of inventory sets require age
			{
				HashSet<String> criterion = new HashSet<String>();
				criterion.add(Ages[rand.nextInt(Ages.length)]);
				some_criteria.putIfAbsent("age", criterion);
			}

			if (rand.nextInt(10) > 1) // only 10% of users have income known
			{
				HashSet<String> criterion = new HashSet<String>();
				criterion.add(Incomes[rand.nextInt(Incomes.length)]);
				some_criteria.putIfAbsent("income", criterion);
			}

			if (rand.nextInt(2) > 1) // only 50% of paves have content defined
			{
				HashSet<String> criterion = new HashSet<String>();
				criterion.add(Contents[rand.nextInt(Contents.length)]);
				some_criteria.putIfAbsent("content", criterion);
			}
			
			set.setcriteria(some_criteria);
			inventorysets.add(set);
		}
		
		// Generate segments
		ArrayList<segment> segments = new ArrayList<segment>();
		for (int ind = 0; ind < segments_count; ind++)
		{
			segment seg = new segment();
			seg.setCount(rand.nextInt(max_segment_count));
			criteria some_criteria = new criteria();

			{
				HashSet<String> criterion = new HashSet<String>();
				criterion.add(Regions[rand.nextInt(Regions.length)]);
				some_criteria.putIfAbsent("region", criterion); // all users have
			}
															// region
			seg.setcriteria(some_criteria);

			if (rand.nextInt(3) > 1) // only 66% of users have interests known
			{
				HashSet<String> criterion = new HashSet<String>();
				while (true) {
					if (!criterion.add(Interests[rand.nextInt(Interests.length)]))
						break;
				}
				some_criteria.putIfAbsent("interests", criterion);
			}

			if (rand.nextInt(2) > 1) // only 50% of users have gender known
			{
				HashSet<String> criterion = new HashSet<String>();
				criterion.add(Genders[rand.nextInt(Genders.length)]);
				some_criteria.putIfAbsent("gender", criterion);
			}

			if (rand.nextInt(5) > 1) // only 20% of users have age known
			{
				HashSet<String> criterion = new HashSet<String>();
				criterion.add(Ages[rand.nextInt(Ages.length)]);
				some_criteria.putIfAbsent("age", criterion);
			}

			if (rand.nextInt(10) > 1) // only 10% of users have income known
			{
				HashSet<String> criterion = new HashSet<String>();
				criterion.add(Incomes[rand.nextInt(Incomes.length)]);
				some_criteria.putIfAbsent("income", criterion);
			}

			if (rand.nextInt(2) > 1) // only 50% of pages have content defined
			{
				HashSet<String> criterion = new HashSet<String>();
				criterion.add(Contents[rand.nextInt(Contents.length)]);
				some_criteria.putIfAbsent("content", criterion);
			}
			
			seg.setcriteria(some_criteria);
			segments.add(seg);
		}
		
		InventroryData inventorydata = new InventroryData();
		inventorydata.setSegments(segments.toArray(new segment[segments.size()]));
		inventorydata.setInventorysets(inventorysets.toArray(new inventoryset[inventorysets.size()]));
		try 
		{
			mapper.writeValue(new File("Test.json"), inventorydata);
		} 
		catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
