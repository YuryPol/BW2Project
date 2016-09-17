package invmanage2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;

import com.bwing.invmanage2.InventroryData;
import com.bwing.invmanage2.criteria;
import com.bwing.invmanage2.inventoryset;
import com.bwing.invmanage2.segment;

public class GenerateInventory {
	
	private static String keys[] = 
		{
				"State"
		};
	
	private static final String[] States = new String[] { "NY", "WA" };
	private static final String[] Interests = new String[] 
			{"sport", "business", "cooking", "movies", "politics", "nature", "cars", "food", "investments", "golf", "vine", "football", "sport", "tech"};
	private static final String[] Genders = new String[] {"F", "M"};
	private static final String[] Ages = new String[] {"child", "young", "middle", "senior"};
	private static final String[] Incomes = new String[] {"poor", "middle", "affluent", "rich"};
	private static final String[] Contents = new String[] {"drama", "news", "actions", "travel"};
	
	//public static final HashSet<String> StatesSet = new HashSet<String>(Arrays.asList(States));
	
	private HashMap<String, HashSet<String>> criterion; 

	public static void main(String[] args) {
		// Generate segments
		segment[] segments = null;
		HashSet<String> some_set = new HashSet<String>();
        Random rand= new Random();
		while (true)
		{
			segment seg = new segment();
			while (true)
			{
				criteria some_criteria = new criteria();
				
				some_set.clear();				
				some_set.add(States[rand.nextInt(States.length)]);
				some_criteria.putIfAbsent("state", some_set);
				seg.setcriteria(some_criteria);
				
				if (rand.nextInt(3) > 1) // only 66% of users have interests known
				{
					some_set.clear();
					while (true)
					{
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
				
				break;
			}
			break;
		}
		
		// Generate sets
		inventoryset[] inventorysets = null;
		
		InventroryData inventorydata = new InventroryData();
		inventorydata.setSegments(segments);
		inventorydata.setInventorysets(inventorysets);
	}

}
