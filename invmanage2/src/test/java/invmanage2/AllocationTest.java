package invmanage2;

import java.sql.SQLException;
import java.sql.Statement;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.sql.ResultSet;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Logger;

import com.bwing.invmanage2.InventoryState;
import com.bwing.invmanage2.RunSimualation;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;

public class AllocationTest {

	public static void main(String[] args)
			throws ClassNotFoundException, SQLException, JsonParseException, JsonMappingException, IOException, InterruptedException 
	{
		Logger log = Logger.getLogger(AllocationTest.class.getName());
		log.info("Testing allocations");

		String file_name = args[0];
		String customer_name = args[1];
		boolean invOnly = false;
		if (args.length > 2 && args[2].equals("only"))
		{
			invOnly = true;
		}
		InputStream in = null;
		ReadableByteChannel readChannel = null;
		ResultSet rs = null;
		if (file_name.length() == 0)
		{
			file_name = "..\\Test.json";
		}
		int repeats =  Integer.parseInt(args[3]);
		boolean simulate = false;
		if (args.length > 4 && args[4].equals("simulate"))
		{
			simulate = true;
		}
		
		try {
			for (int ind = 0; ind < repeats; ind++) {
				if (file_name.equals("..\\Test.json"))
				{
			        Random rand = new Random();
					int inventorysets_count = rand.nextInt(InventoryState.BITMAP_MAX_SIZE -10) + 10; // at least 10 inventory sets
					int segments_count = rand.nextInt(15000 - 100) + 100;
					int max_segment_count = rand.nextInt(500); //000 / segments_count);

					InventoryGenerator.doIt(inventorysets_count, segments_count, max_segment_count);
				}
				// Initialize inventory
				InventoryState invState = new InventoryState(customer_name, true);
				invState.clear();
				// Process the file

				// opens a file to read from the given location
				in = new FileInputStream(file_name);

				// returns ReadableByteChannel instance to read the file
				readChannel = Channels.newChannel(in);

				invState.loadDynamic(readChannel, false);
				log.info(file_name + " was parsed successfuly.");

				if (!invState.isLoaded()) {
					log.severe("The inventory " + customer_name + " is " + invState.getStatus()
							+ ". Nothing to allocate");
					invState.close();
					continue;
				}
				else if (invOnly)
				{
					log.info("Inventory build only, exiting");
					invState.close();
					return;
				}
				else 
				{
					log.info("Running allocations");
					Statement st = invState.getConnection().createStatement();
					st.execute("USE " + InventoryState.BWdb + customer_name);
					while (true) {
						rs = st.executeQuery("SELECT count(*) FROM structured_data_base WHERE availability > 0");
						rs.next();
						int count = rs.getInt(1);
						if (count == 0) {
							log.info("Allocations completed sucessfuly");
//							invState.close();
							break;
						}

						// check totals
						rs = st.executeQuery("SELECT SUM(goal) FROM structured_data_base");
						rs.next();
						int totalGoal = rs.getInt(1);
						rs = st.executeQuery("SELECT SUM(availability) FROM structured_data_base");
						rs.next();
						int total_availability = rs.getInt(1);
						rs = st.executeQuery("SELECT SUM(capacity) FROM structured_data_base");
						rs.next();
						int total_capacity = rs.getInt(1);
						if (total_capacity < totalGoal + total_availability) 
						{
							log.severe("There is a problem with capacity, goal, availability on repeat #" + ind);
							log.severe(Integer.toString(total_capacity) + ", " + Integer.toString(totalGoal) + ", "
									+ Integer.toString(total_availability));
							invState.close();
							in.close();
							rs.close();
							return;
						}
						rs = st.executeQuery("SELECT SUM(count) FROM raw_inventory");
						rs.next();
						int total_inventory = rs.getInt(1);
						if (total_inventory < totalGoal)
						{
							log.severe("There is a problem with inventory, goalon on repeat #" + ind);
							log.severe(Integer.toString(total_inventory) + ", " + Integer.toString(totalGoal));
							invState.close();
							in.close();
							rs.close();
							return;
						}

						rs = st.executeQuery(
								"SELECT set_name, capacity, availability, goal FROM structured_data_base WHERE availability < 0");
						while (rs.next()) 
						{
							String set_name = rs.getString(1);
							int capacity = rs.getInt(2);
							int availability = rs.getInt(3);
							log.severe(set_name + " with capacity " + Integer.toString(capacity) 
							+ " have negative availability=" + Integer.toString(availability) + " on repeat #" + Integer.toString(ind));
							invState.close();
							in.close();
							rs.close();
							return;
						}
						
						// get one
						rs = st.executeQuery(
								"SELECT set_name, capacity, availability, goal FROM structured_data_base WHERE availability > 0");
						rs.next();
						int current = ThreadLocalRandom.current().nextInt(1, count + 1);
						if (current > 1) {
							rs.relative(current - 1);
						}
						String set_name = rs.getString("set_name");
						int availability = rs.getInt("availability");
						int alloc_Amount = ThreadLocalRandom.current().nextInt(1, availability + 1);

						log.info("Allocate on " + set_name + " : " + Integer.toString(alloc_Amount));
						boolean success = invState.GetItems(set_name, "", alloc_Amount);
						if (success)
							log.info("ammount=" + Integer.toString(alloc_Amount) + " was allocated successfuly");
						else
							log.severe("allocation of ammount=" + Integer.toString(alloc_Amount) + " failed");

						rs = st.executeQuery("SELECT set_name, capacity, goal, availability FROM structured_data_base WHERE availability < 0");
						if (rs.next()) {
							log.severe("Availabilites went negative");
							log.severe("name, capacity, goal, availability");
							do {
								log.severe(rs.getString(1) + " , " + Integer.toString(rs.getInt(2)) + " , "
										+ Integer.toString(rs.getInt(3)) + " , " + Integer.toString(rs.getInt(4)) +  "on repeat #" + Integer.toString(ind));
							} while (rs.next());
							invState.close();
							in.close();
							rs.close();
							return;
						}
					}					
					if (simulate 
							// Run simulation
							&& !RunSimualation.runIt(invState, true))
					{
						log.severe(" on repeat #" + Integer.toString(ind));
						// exit if simulation failed
						invState.close();
						in.close();
						rs.close();
						return;
					}
				}
				invState.close();
				in.close();
				if (rs != null)
					rs.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
