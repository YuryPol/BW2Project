package com.bwing.invmanage2;

import java.sql.SQLException;
import java.sql.Statement;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.sql.ResultSet;

import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Logger;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.google.appengine.tools.cloudstorage.GcsFilename;
import com.google.appengine.tools.cloudstorage.GcsInputChannel;
import com.google.appengine.tools.cloudstorage.GcsService;
import com.google.appengine.tools.cloudstorage.GcsServiceFactory;
import com.google.appengine.tools.cloudstorage.RetryParams;

public class AllocationTest {

	public static void main(String[] args)
			throws ClassNotFoundException, SQLException, JsonParseException, JsonMappingException, IOException {
		Logger log = Logger.getLogger(AllocationTest.class.getName());
		log.info("Testing allocations");

		String file_name = args[0];
		String customer_name = args[1];

		// Initialize inventory
		InventoryState invState = new InventoryState(customer_name);
		invState.lock();
		invState.clear();
		// Process the file

		InputStream in = null;
		ReadableByteChannel readChannel = null;
		ResultSet rs = null;
		try {
			// opens a file to read from the given location
			in = new FileInputStream(file_name);

			// returns ReadableByteChannel instance to read the file
			readChannel = Channels.newChannel(in);

			for (int ind = 0; ind < 100; ind++) {
				invState.load(readChannel);
				log.info(file_name + " was parsed successfuly.");

				if (!invState.isLoaded()) {
					log.warning("The inventory " + customer_name + " is " + invState.getStatus().name()
							+ ". Nothing to allocate");
				} else {
					Statement st = invState.getConnection().createStatement();
					st.execute("USE " + InventoryState.BWdb + customer_name);
					while (true) {
						rs = st.executeQuery("SELECT count(*) FROM structured_data_base WHERE availability > 0");
						rs.next();
						int count = rs.getInt(1);
						if (count == 0) {
							log.info("Allocations completed sucessfuly");

							// check totals
							rs.close();
							rs = st.executeQuery("SELECT SUM(goal) FROM structured_data_base");
							rs.next();
							int totalGoal = rs.getInt(1);
							rs = st.executeQuery("SELECT SUM(availability) FROM structured_data_base");
							rs.next();
							int total_availability = rs.getInt(1);
							rs = st.executeQuery("SELECT SUM(count) FROM raw_inventory");
							rs.next();
							int total_capacity = rs.getInt(1);
							if (total_capacity != totalGoal + total_availability) {
								log.severe("But there is a problem with capacity, goal, availability");
								log.severe(Integer.toString(total_capacity) + ", " + Integer.toString(totalGoal) + ", "
										+ Integer.toString(total_availability));

							}
							invState.close();
							rs.close();
							break;
						}
						int current = ThreadLocalRandom.current().nextInt(1, count + 1);
						rs.close();
						rs = st.executeQuery(
								"SELECT set_name, capacity, availability, goal FROM structured_data_base WHERE availability > 0");
						if (!rs.next()) {
							log.severe("something bad happen");
							invState.close();
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

						log.info(set_name + " : " + Integer.toString(alloc_Amount));
						invState.GetItems(set_name, alloc_Amount);

						rs = st.executeQuery("SELECT set_name FROM structured_data_base WHERE availability < 0");
						if (rs.next()) {
							log.severe("name, capacity, goal, availability");
							do {
								log.severe(rs.getString(1) + " , " + Integer.toString(rs.getInt(2)) + " , "
										+ Integer.toString(rs.getInt(3)) + " , " + Integer.toString(rs.getInt(4)));
							} while (rs.next());
						}
						rs.close();
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			in.close();
			readChannel.close();
			rs.close();
			invState.close();
		}
	}
}
