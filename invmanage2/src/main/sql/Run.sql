/* 
 * Runs at every session
 */
 
USE Demo;

-- create temporary table to insert next rank rows
DROP /*TEMPORARY*/ TABLE IF EXISTS unions_last_rank;
CREATE /*TEMPORARY*/ TABLE unions_last_rank(
    set_key BIGINT DEFAULT NULL,
    set_name VARCHAR(20) DEFAULT NULL,
--    rank INT DEFAULT NULL,
    capacity INT DEFAULT NULL, 
    availability INT DEFAULT NULL, 
    goal INT DEFAULT 0,
    PRIMARY KEY(set_key)) /*ENGINE=MEMORY*/
;

DROP /*TEMPORARY*/ TABLE IF EXISTS unions_next_rank;
CREATE /*TEMPORARY*/ TABLE unions_next_rank(
    set_key BIGINT DEFAULT NULL,
    set_name VARCHAR(20) DEFAULT NULL,
--    rank INT DEFAULT NULL,
    capacity INT DEFAULT NULL, 
    availability INT DEFAULT NULL, 
    goal INT DEFAULT 0,
    PRIMARY KEY(set_key)) /*ENGINE=MEMORY*/
;

DROP TABLE IF EXISTS raw_inventory;
CREATE TABLE raw_inventory AS 
SELECT basesets, sum(count) as count
   FROM raw_inventory_ex
   GROUP BY basesets; -- adds up multiple records in raw_inventory_ex with the same key
   -- That shouldn't be necessary as raw_inventory_ex already groups them but verification is needed.
   
-- update raw inventory with weights
ALTER TABLE raw_inventory ADD weight BIGINT DEFAULT '0' NOT NULL;
SELECT @n:=0;
UPDATE raw_inventory SET weight = @n := @n + raw_inventory.count;
   
-- that adds capacities and availabilities to structured data
UPDATE structured_data_base sdb,
  (SELECT set_key, SUM(ri.count) AS capacity, SUM(ri.count) AS availability
   FROM structured_data_base
   JOIN raw_inventory ri
   ON set_key & ri.basesets != 0
   GROUP BY set_key) comp
 SET sdb.capacity = comp.capacity,
     sdb.availability = comp.availability
 WHERE sdb.set_key = comp.set_key;

-- populate inventory sets table
TRUNCATE structured_data_inc;
INSERT INTO structured_data_inc 
   SELECT set_key  -- inventory set's key
   , set_name, capacity, availability, goal
   FROM structured_data_base
   WHERE capacity IS NOT NULL;
    
-- start union_next_rank table
TRUNCATE unions_next_rank;
INSERT INTO unions_next_rank 
   SELECT *
   FROM structured_data_inc;

-- SET max_sp_recursion_depth=255;
CALL AddUnions; -- creates unions
