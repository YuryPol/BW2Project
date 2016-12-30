    public void load(ReadableByteChannel readChannel) throws JsonParseException, JsonMappingException, IOException, SQLException
    {
    	clear();
    	//convert json input to InventroryData object
		InventroryData inventorydata= mapper.readValue(Channels.newInputStream(readChannel), InventroryData.class);
		if (inventorydata.getSegments().length > BITMAP_SIZE)
		{
			log.severe(customer_name + " :  There are " + String.valueOf(inventorydata.getSegments().length) + " (more than allowed " + String.valueOf(BITMAP_SIZE) + ") inventory sets in " + readChannel.toString());
			wrongFile();
			return;
		}
		// Create inventory sets data. TODO: write into DB from the start
		HashMap<BitSet, BaseSet> base_sets = new HashMap<BitSet, BaseSet>();			
		int highBit = 0;
		for (segment is : inventorydata.getSegments())
		{
			boolean match_found = false;
			for (BaseSet bs1 : base_sets.values())
			{
/*				if (bs1.getCriteria() == null && is.getcriteria() == null)
				{
					match_found = true;
					break;
				}
				else if (bs1.getCriteria() == null)
				{
					// because is.criteria isn't null no need to compare
					continue;
				}
				else */
				if (criteria.equals(bs1.getCriteria(), is.getCriteria()))
				{
					match_found = true;
					break;
				}
			}
			if (match_found)
				continue; // skip repeated set
			
			BaseSet tmp = new BaseSet(BITMAP_SIZE);
			tmp.setkey(highBit);
			tmp.setname(is.getName());
			tmp.setCriteria(is.getCriteria());
			base_sets.put(tmp.getkey(), tmp);
			highBit++;
		}
		if (highBit == 0)
		{
			log.severe(customer_name + " :  no data in inventory sets in " + readChannel.toString());
			wrongFile();
			return;
		}			
		
		// Create segments' raw data. TODO: write into DB from the start
		HashMap<BitSet, BaseSegement> base_segments = new HashMap<BitSet, BaseSegement>();
		for (opportunity seg : inventorydata.getOpportunities())
		{
			boolean match_found = false;
			BaseSegement tmp = new BaseSegement(BITMAP_SIZE);
			tmp.setCriteria(seg.getcriteria());
			
			for (BaseSet bs1 : base_sets.values())
			{					
				if (bs1.getCriteria() == null && tmp.getCriteria() == null)
				{
					tmp.getkey().or(bs1.getkey());
					match_found = true;
				}
				else if (bs1.getCriteria() == null)
				{
					// because tmp.criteria isn't null no need to compare
					continue;
				}
				else if (criteria.matches(bs1.getCriteria(), tmp.getCriteria()))
				{
					tmp.getkey().or(bs1.getkey());
					match_found = true;
				}
			}
			if (match_found) 
			{
				int capacity = seg.getCount();
				BaseSegement existing = null;
				if ((existing = base_segments.get(tmp.getkey())) != null)
				{
					capacity += existing.getcapacity();
				}
				tmp.setcapacity(capacity);
				base_segments.put(tmp.getkey(), tmp);
			}
		}
		if (base_segments.isEmpty())
		{
			log.severe(customer_name + " :  no data in segments " + readChannel.toString());
			wrongFile();
			return;
		}
		else if (base_segments.size() > INVENTORY_OVERLAP)
		{
			log.severe(customer_name + " : segments overlap is too high, " + Integer.toString(base_segments.size()) + " for file " + readChannel.toString());
			wrongFile();
			return;
		}

		//
		// Populate all tables 
		//
        // populate structured data with inventory sets
        try (PreparedStatement insertStatement = con.prepareStatement("INSERT IGNORE INTO "  + structured_data_base 
        		+ " SET set_key = ?, set_name = ?, set_key_is = ?, criteria = ?"))
        {
	        for (BaseSet bs1 : base_sets.values()) {
	         	insertStatement.setLong(1, bs1.getKeyBin()[0]);
	            insertStatement.setString(2, bs1.getname());
	        	insertStatement.setLong(3, bs1.getKeyBin()[0]);
	        	if (bs1.getCriteria() == null)
	        	{
		        	insertStatement.setString(4, "");	        		
	        	}
	        	else
	        	{
	        		insertStatement.setString(4, bs1.getCriteria().toString());
	        	}	            
	        	insertStatement.execute();
	        }
        }
        
        // populate raw data with inventory sets' bitmaps
        try (PreparedStatement insertStatement = con.prepareStatement("INSERT INTO "  + raw_inventory 
        		+ " SET basesets = ?, count = ?, criteria = ?, weight = ? ON DUPLICATE KEY UPDATE count = VALUES(count) + count" ))
        {
        	log.info(customer_name + " :  INSERT INTO "  + raw_inventory);
	        for (BaseSegement bs1 : base_segments.values()) {
	        	insertStatement.setLong(1, bs1.getKeyBin()[0]);
	        	insertStatement.setInt(2, bs1.getcapacity());
	        	if (bs1.getCriteria() == null)
	        	{
		        	insertStatement.setString(3, "");	        		
	        	}
	        	else
	        	{
	        		insertStatement.setString(3, bs1.getCriteria().toString());
	        	}
	        	insertStatement.setLong(4, 0);
	            insertStatement.execute();
	        }
        }
                
        // update raw inventory with weights
        try (PreparedStatement st = con.prepareStatement("SELECT @n:=0"))
        {
        	st.execute();
        }
        
        try (PreparedStatement st = con.prepareStatement("UPDATE " + raw_inventory
        		+ " SET weight = @n := @n + " + raw_inventory + ".count"))
        {
        	log.info( customer_name + " : UPDATE " + raw_inventory);
        	st.execute();
        }

        // adds capacities and availabilities to structured data
        try (Statement st = con.createStatement())
        {
        	
        	st.executeUpdate("UPDATE "
        			+ structured_data_base + " AS sdbW, "
        			+ " (SELECT set_key, SUM(ri.count) AS capacity, SUM(ri.count) AS availability FROM "
        			+ structured_data_base + " AS sdbR "
        			+ " JOIN " 
        			+ raw_inventory + " AS ri "
        			+ " ON set_key & ri.basesets != 0 "
        			+ " GROUP BY set_key) comp "
        			+ " SET sdbW.capacity = comp.capacity, "
        			+ " sdbW.availability = comp.availability "
        			+ " WHERE sdbW.set_key = comp.set_key");
        	log.info(customer_name + " : UPDATE " + structured_data_base);
        	
        	// populate inventory sets table
        	st.executeUpdate("INSERT INTO " 
        			+ structured_data_inc
        			+ " SELECT set_key, set_name, capacity, availability, goal FROM " 
        			+ structured_data_base 
        			+ " WHERE capacity IS NOT NULL");
        	log.info(customer_name + " : INSERT INTO " + structured_data_inc);
        }
        
        // start union_next_rank table
        try (PreparedStatement insertStatement = con.prepareStatement("INSERT INTO " + unions_next_rank
        		+ " SELECT * FROM " + structured_data_inc))
        {
        	log.info(customer_name + " : INSERT INTO " + unions_next_rank);
        	insertStatement.execute();        	
        }
        
        // adds unions of higher ranks for all nodes to structured_data_inc
        try (CallableStatement callStatement = con.prepareCall("{call AddUnions}"))
        {
        	log.info(customer_name + " :  {call AddUnions}");
           	callStatement.executeUpdate();
        }
        catch (CommunicationsException ex)
        {
        	// TODO: ignoring it for now until we figure out how to set timeout higher than 5 sec.
        	log.severe(customer_name + " : AddUnions thrown " + ex.getMessage());
        }
//        catch (Exception ex)
//        {
//        	log.severe(ex.getMessage());
//        	throw ex;
//        }
        log.info(customer_name + " : Inventory was loaded!");
    }

    
    // Old stored proc.
	st.executeUpdate("DROP PROCEDURE IF EXISTS AddUnions");
	st.executeUpdate("CREATE PROCEDURE AddUnions() "
			+ "BEGIN "
			+ "    DECLARE cnt INT;"
			+ "    DECLARE cnt_updated INT;"
			
			+ "    REPEAT "
			
			+ "    SELECT count(*) INTO cnt FROM structured_data_inc;"
			
			+ "    TRUNCATE unions_last_rank;"
			
			+ "    INSERT INTO unions_last_rank "
			+ "	   SELECT * FROM unions_next_rank;"
			
			+ "	TRUNCATE unions_next_rank;"
			
			+ "	INSERT /*IGNORE*/ INTO unions_next_rank "
			+ "    SELECT sdbR.set_key_is | unions_last_rank.set_key, NULL, NULL, NULL, 0 "
			+ "	   FROM unions_last_rank "
			+ "    JOIN structured_data_base sdbR "
			+ "	   JOIN raw_inventory ri "
			+ "    ON  (sdbR.set_key_is & ri.basesets != 0) "
			+ "        AND (unions_last_rank.set_key & ri.basesets) != 0 "
			+ "        AND (sdbR.set_key_is | unions_last_rank.set_key) != unions_last_rank.set_key "
			+ "    GROUP BY sdbR.set_key_is | unions_last_rank.set_key;"
			
			+ " CALL PopulateRankWithNumbers;"
			
			+ " DELETE FROM structured_data_inc "
			+ "    WHERE EXISTS ("
			+ "        SELECT * "
			+ "        FROM unions_next_rank unr1"
			+ "        WHERE (structured_data_inc.set_key & unr1.set_key) = structured_data_inc.set_key "
			+ "        AND structured_data_inc.capacity = unr1.capacity); "
			
			+ " INSERT /*IGNORE*/ INTO structured_data_inc "
			+ "    SELECT * FROM unions_next_rank;"
			
			+ " SELECT count(*) INTO cnt_updated FROM structured_data_inc; "
			
			+ " UNTIL  (cnt = cnt_updated) "
			
			+ " END REPEAT; "
			
			+ " DELETE FROM structured_data_inc "
			+ "    WHERE capacity IS NULL; "
			
			+ " UPDATE structured_data_base, structured_data_inc "
			+ "    SET structured_data_base.set_key = structured_data_inc.set_key "
			+ "    WHERE structured_data_base.set_key_is & structured_data_inc.set_key = structured_data_base.set_key_is "
			+ "    AND structured_data_base.capacity = structured_data_inc.capacity; "
			
			// Validate the data in DB
        	+ " REPLACE INTO " + inventory_status + " VALUES(1, '" + Status.loaded.name() + "');"       			
			
			+ "END "
			);
