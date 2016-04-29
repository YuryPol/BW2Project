/* 
 * Runs when updated
 * Creates needed stored procs
 */
 
CREATE DATABASE IF NOT EXISTS Demo;

USE Demo;

--
-- setting up stored procs
--

DROP PROCEDURE IF EXISTS test1;
DELIMITER //
CREATE  PROCEDURE test1(IN tab_name VARCHAR(40) )
BEGIN
 SET @t1 = CONCAT('SELECT * FROM ',tab_name );
 PREPARE stmt3 FROM @t1;
 EXECUTE stmt3;
 DEALLOCATE PREPARE stmt3;
END //
DELIMITER ;

-- adds capacities, availabilities
DROP PROCEDURE IF EXISTS PopulateRankWithNumbers;
DELIMITER //
CREATE PROCEDURE PopulateRankWithNumbers(IN unions_next_rank VARCHAR(40), IN raw_inventory VARCHAR(40))
BEGIN
 SET @t1 = CONCAT(' 
 UPDATE ', unions_next_rank, ' nr0,
 (SELECT 
    set_key,
    SUM(capacity) as capacity, 
    SUM(availability) as availability
  FROM (    
   SELECT nr.set_key, ri.count as capacity, ri.count as availability
    FROM ', unions_next_rank, ' nr 
    JOIN ', raw_inventory, ' ri 
    ON nr.set_key & ri.basesets != 0 
    WHERE nr.capacity is NULL
    ) blownUp
  GROUP BY set_key
 ) comp
 SET nr0.capacity = comp.capacity,
     nr0.availability = comp.availability
 WHERE nr0.set_key = comp.set_key')
 ;
 PREPARE stmt3 FROM @t1;
 EXECUTE stmt3;
 DEALLOCATE PREPARE stmt3;
END //
DELIMITER ;

-- adds unions of higher ranks for all nodes
DROP PROCEDURE IF EXISTS AddUnions;
DELIMITER //
CREATE PROCEDURE AddUnions()
BEGIN
    DECLARE cnt INT;   
    DECLARE cnt_updated INT;
    REPEAT 
    SELECT count(*) INTO cnt FROM structured_data_inc;
    -- save rank table created in previous itteration
    TRUNCATE unions_last_rank;
    INSERT INTO unions_last_rank
	   SELECT * FROM unions_next_rank;
	TRUNCATE unions_next_rank;
	-- build next rank
	INSERT /*IGNORE*/ INTO unions_next_rank
       SELECT sb.set_key_is | lr.set_key, NULL, NULL, NULL, 0
	   FROM unions_last_rank lr
       JOIN structured_data_base sb
	   JOIN raw_inventory ri
           ON  (sb.set_key_is & ri.basesets != 0)
           AND (lr.set_key & ri.basesets) != 0
           AND (sb.set_key_is | lr.set_key) != lr.set_key
       GROUP BY sb.set_key_is | lr.set_key;
    CALL PopulateRankWithNumbers;
    -- delete fully included sets of lower rank
    DELETE FROM structured_data_inc
    WHERE EXISTS (
        SELECT *
        FROM unions_next_rank nr
        WHERE (structured_data_inc.set_key & nr.set_key) = structured_data_inc.set_key
        -- AND structured_data_inc.set_key != nr.set_key
        AND structured_data_inc.capacity = nr.capacity);    
    -- add temp table to sturctured data
    INSERT /*IGNORE*/ INTO structured_data_inc
    SELECT * FROM unions_next_rank;
     -- Continue adding unions of higher ranks
    SELECT count(*) INTO cnt_updated FROM structured_data_inc; 
    UNTIL  (cnt = cnt_updated) 
    END REPEAT;
    -- delete empty sets ???? Not here.
    DELETE FROM structured_data_inc
    WHERE capacity IS NULL;    
    -- link from structured_data_base
    UPDATE structured_data_base, structured_data_inc
    SET structured_data_base.set_key = structured_data_inc.set_key
    WHERE structured_data_base.set_key_is & structured_data_inc.set_key = structured_data_base.set_key_is
    AND structured_data_base.capacity = structured_data_inc.capacity;
END //
DELIMITER ;

-- book an amount of items
DROP FUNCTION IF EXISTS BookItemsFromIS;
DELIMITER //
CREATE FUNCTION BookItemsFromIS(iset BIGINT, amount INT)
RETURNS BOOLEAN
DETERMINISTIC
READS SQL DATA
BEGIN
    DECLARE cnt INT;   
    SELECT availability INTO cnt FROM structured_data_base WHERE set_key_is = iset; 
    IF cnt >= amount AND amount > 0
    THEN
     UPDATE structured_data_base 
     SET availability=availability-amount, goal=goal+amount 
     WHERE set_key_is = iset;
     RETURN TRUE;
    ELSE
     RETURN FALSE;
    END IF;
END //
DELIMITER ;

-- getting an amount of items from structured data, second try with deleting lower order nodes.
DROP PROCEDURE IF EXISTS GetItemsFromSD;
DELIMITER //
CREATE PROCEDURE GetItemsFromSD(IN iset BIGINT, IN amount INT)
BEGIN
   IF BookItemsFromIS(iset, amount)
   THEN
     -- update structured data with rule #1
     UPDATE structured_data_inc 
        SET availability = availability - amount 
        WHERE (set_key & iset) = iset;
     -- update structured data with rule #2
       DELETE FROM structured_data_inc WHERE set_key = ANY (
       SELECT set_key FROM (
          SELECT sd1.set_key
          FROM structured_data_inc sd1 JOIN structured_data_inc sd2
          ON sd2.set_key > sd1.set_key 
          AND sd2.set_key & sd1.set_key = sd1.set_key 
          AND sd1.availability >= sd2.availability) AS stmp
       );
     -- propagate the changes into base table
     UPDATE structured_data_base sb, structured_data_inc sd
     SET sb.availability = LEAST(sb.availability, sd.availability)
     WHERE sd.set_key & sb.set_key_is = sb.set_key_is;
     SELECT 'passed';
   ELSE
     SELECT 'failed';
   END IF;
END //
DELIMITER ;

