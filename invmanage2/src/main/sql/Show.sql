INSERT IGNORE INTO unions_next_rank
SELECT structured_data_base.set_key_is | unions_last_rank.set_key, NULL, NULL, NULL, 0 
FROM unions_last_rank    
JOIN structured_data_base
JOIN raw_inventory    
ON  (structured_data_base.set_key_is & unions_last_rank.set_key = 0)
AND (unions_last_rank.set_key & raw_inventory.basesets) = 0
AND (structured_data_base.set_key_is & raw_inventory.basesets) > 0
AND structured_data_base.set_key_is IN
( 
SELECT set_key_is              
FROM structured_data_base 
JOIN raw_inventory JOIN unions_last_rank             
ON structured_data_base.set_key_is & raw_inventory.basesets & unions_last_rank.set_key > 0
);


select lpad(bin(set_key_is), 20, '0') as set_key_is, lpad(bin(set_key), 20, '0') as set_key, set_name, capacity, availability, goal, criteria from structured_data_base;
select lpad(bin(set_key), 20, '0') as set_key, set_name, capacity, availability, goal from structured_data_inc order by set_key;

select set_key_is, set_name, capacity, availability, goal from structured_data_base;
call GetItemsFromSD(
1,10);

select lpad(bin(set_key), 20, '0') as set_key, set_name, capacity, availability, goal from unions_next_rank;

select lpad(bin(basesets), 20, '0') as basesets, count from raw_inventory order by basesets;

select lpad(bin(l_key), 20, '0') as l_key, lpad(bin(n_key), 20, '0') as n_key, capacity from ex_inc_unions;

select lpad(bin(basesets), 10, '0') as basesets, count, criteria from raw_inventory_ex;   

select lpad(bin(set_key_is), 10, '0') as set_key_is, lpad(bin(set_key), 10, '0') as baseset from result_serving;

select set_key_is, lpad(bin(set_key), 10, '0') as set_key, capacity, availability, goal, served_count from result_serving;

--
-- testing aids
--

DROP PROCEDURE IF EXISTS GetTotalCapacity;
DELIMITER //
CREATE PROCEDURE GetTotalCapacity()
BEGIN
  SELECT 
    SUM(count) as total_capacity
  FROM raw_inventory; 
END //
DELIMITER ;

DROP PROCEDURE IF EXISTS GetTotalAvailability;
DELIMITER //
CREATE PROCEDURE GetTotalAvailability()
BEGIN
  SELECT 
    SUM(availability) as total_availability
  FROM structured_data_base; 
END //
DELIMITER ;

DROP PROCEDURE IF EXISTS GetTotalGoals;
DELIMITER //
CREATE PROCEDURE GetTotalGoals()
BEGIN
  SELECT 
    SUM(goal) as total_goals
  FROM structured_data_base; 
END //
DELIMITER ;


INSERT /*IGNORE*/ INTO unions_next_rank    
SELECT structured_data_base.set_key_is | unions_last_rank.set_key, NULL, NULL, NULL, 0 
FROM unions_last_rank    
JOIN structured_data_base
JOIN raw_inventory    
ON  (structured_data_base.set_key_is & raw_inventory.basesets != 0)         
AND (unions_last_rank.set_key & raw_inventory.basesets) != 0         
AND (structured_data_base.set_key_is & unions_last_rank.set_key) == 0     
GROUP BY structured_data_base.set_key_is | unions_last_rank.set_key;
