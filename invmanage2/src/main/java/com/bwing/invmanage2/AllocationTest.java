package com.bwing.invmanage2;

import java.sql.SQLException;
import java.sql.Statement;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.sql.ResultSet;

import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Logger;

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
		if (args.length > 2 && args[2] == "only")
		{
			invOnly = true;
		}
		InputStream in = null;
		ReadableByteChannel readChannel = null;
		ResultSet rs = null;

		try {
			for (int ind = 0; ind < 100; ind++) {
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
					log.warning("The inventory " + customer_name + " is " + invState.getStatus()
							+ ". Nothing to allocate");
					invState.close();
					return;
				}
				else if (invOnly)
				{
					log.info("Inventory build only, exiting");
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
							invState.close();
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
							log.severe("There is a problem with capacity, goal, availability");
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
							log.severe("There is a problem with inventory, goal");
							log.severe(Integer.toString(total_inventory) + ", " + Integer.toString(totalGoal));
							invState.close();
							in.close();
							rs.close();
							return;
						}

						int current = ThreadLocalRandom.current().nextInt(1, count + 1);
						rs = st.executeQuery(
								"SELECT set_name, capacity, availability, goal FROM structured_data_base WHERE availability > 0");
						if (!rs.next()) {
							log.severe("something bad happen");
							invState.close();
							in.close();
							rs.close();
							return;
						}
						// get one
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

						rs = st.executeQuery("SELECT set_name FROM structured_data_base WHERE availability < 0");
						if (rs.next()) {
							log.severe("Availabilites went negative");
							log.severe("name, capacity, goal, availability");
							do {
								log.severe(rs.getString(1) + " , " + Integer.toString(rs.getInt(2)) + " , "
										+ Integer.toString(rs.getInt(3)) + " , " + Integer.toString(rs.getInt(4)));
							} while (rs.next());
							invState.close();
							in.close();
							rs.close();
							return;
						}
					}
				}
				invState.close();
				in.close();
				if (rs != null)
					rs.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
