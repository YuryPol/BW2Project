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
import com.bwing.invmanage2.segment;
import com.bwing.invmanage2.opportunity;
import com.fasterxml.jackson.databind.ObjectMapper;

public class GenerateInventory 
{	
	private static final String[] Regions = new String[] { "Northeast", "Midwest", "South", "West" };
	private static final String[] Interests = new String[] 
			{"sport", "business", "cooking", "movies", "politics", "nature", "cars", "food", "investments", "golf", "vine", "football", "sport", "tech"};
	private static final String[] Genders = new String[] {"F", "M"};
	private static final String[] Ages = new String[] {"young", "middle", "senior"};
	private static final String[] Incomes = new String[] {"middle", "affluent", "rich"};
	private static final String[] Contents = new String[] {"drama", "comedy", "news", "actions", "travel"};
	
    private static ObjectMapper mapper = new ObjectMapper();
	
	public static void main(String[] args) {
		int inventorysets_count = Integer.parseInt(args[0]);
		int segments_count = Integer.parseInt(args[1]);
		int max_segment_count = Integer.parseInt(args[2]);
        Random rand = new Random();
		
		// Generate inventory sets
		ArrayList<segment> inventorysets = new ArrayList<segment>();
		for (int ind = 0; ind < inventorysets_count; ind++)
		{
			segment set = new segment();
			criteria some_criteria = new criteria();
			set.setName("Audience_" + Integer.toString(ind, 10));

			if (rand.nextInt(10) >= 8) // only 20% of inventory sets require region
			{
				HashSet<String> criterion = new HashSet<String>();
				while (true) 
				{
					if (!criterion.add(Regions[rand.nextInt(Regions.length)]))
						break;
				}
				some_criteria.putIfAbsent("regions", criterion);
			}

			if (rand.nextInt(10) >= 7) // only 30% of inventory sets require interests
			{
				HashSet<String> criterion = new HashSet<String>();
				while (true) 
				{
					if (!criterion.add(Interests[rand.nextInt(Interests.length)]) || rand.nextInt(3) >= 1)
						// 2 interests on average per segment
						break;
				}
				some_criteria.putIfAbsent("interests", criterion);
			}

			if (rand.nextInt(10) >= 9) // only 10% of inventory sets require gender
			{
				HashSet<String> criterion = new HashSet<String>();
				criterion.add(Genders[rand.nextInt(Genders.length)]);
				some_criteria.putIfAbsent("gender", criterion);
			}

			if (rand.nextInt(10) >= 8) // only 20% of inventory sets require age
			{
				HashSet<String> criterion = new HashSet<String>();
				criterion.add(Ages[rand.nextInt(Ages.length)]);
				some_criteria.putIfAbsent("age", criterion);
			}

			if (rand.nextInt(10) >= 8) // only 20% of inventory sets require income
			{
				HashSet<String> criterion = new HashSet<String>();
				criterion.add(Incomes[rand.nextInt(Incomes.length)]);
				some_criteria.putIfAbsent("income", criterion);
			}

			if (rand.nextInt(2) >= 1) // only 50% of inventory sets require content
			{
				HashSet<String> criterion = new HashSet<String>();
				criterion.add(Contents[rand.nextInt(Contents.length)]);
				some_criteria.putIfAbsent("content", criterion);
			}
			
			set.setcriteria(some_criteria);
			inventorysets.add(set);
		}
		
		// Generate segments
		ArrayList<opportunity> segments = new ArrayList<opportunity>();
		for (int ind = 0; ind < segments_count; ind++)
		{
			opportunity seg = new opportunity();
			seg.setCount(rand.nextInt(max_segment_count));
			criteria some_criteria = new criteria();

			{
				HashSet<String> criterion = new HashSet<String>();
				criterion.add(Regions[rand.nextInt(Regions.length)]);
				some_criteria.putIfAbsent("regions", criterion); // all users have region
			}

			seg.setcriteria(some_criteria);

			if (rand.nextInt(3) >= 1) // only 66% of users have interests known
			{
				HashSet<String> criterion = new HashSet<String>();
				while (true) {
					if (!criterion.add(Interests[rand.nextInt(Interests.length)]) || rand.nextInt(3) >= 1)
						// 2 interests on average per person
						break;
				}
				some_criteria.putIfAbsent("interests", criterion);
			}

			if (rand.nextInt(2) >= 1) // only 50% of users have gender known
			{
				HashSet<String> criterion = new HashSet<String>();
				criterion.add(Genders[rand.nextInt(Genders.length)]);
				some_criteria.putIfAbsent("gender", criterion);
			}

			if (rand.nextInt(2) >= 1) // only 50% of users have age known
			{
				HashSet<String> criterion = new HashSet<String>();
				criterion.add(Ages[rand.nextInt(Ages.length)]);
				some_criteria.putIfAbsent("age", criterion);
			}

			if (rand.nextInt(2) >= 1) // only 50% of users have income known
			{
				HashSet<String> criterion = new HashSet<String>();
				criterion.add(Incomes[rand.nextInt(Incomes.length)]);
				some_criteria.putIfAbsent("income", criterion);
			}

			if (rand.nextInt(10) >= 1) // only 90% of pages have content defined
			{
				HashSet<String> criterion = new HashSet<String>();

				while (true) {
					if (!criterion.add(Contents[rand.nextInt(Contents.length)]) || rand.nextInt(3) >= 1)
						// 2 contents on average per page
						break;
				}
				some_criteria.putIfAbsent("content", criterion);
			}
			
			seg.setcriteria(some_criteria);
			segments.add(seg);
		}
		
		InventroryData inventorydata = new InventroryData();
		inventorydata.setOpportunities(segments.toArray(new opportunity[segments.size()]));
		inventorydata.setInventorysets(inventorysets.toArray(new segment[inventorysets.size()]));
		try 
		{
			mapper.writeValue(new File("..\\Test.json"), inventorydata);
		} 
		catch (IOException e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
