select set_key_is, lpad(bin(set_key), 10, '0') as set_key, set_name, capacity, availability, goal, criteria from structured_data_base;
select lpad(bin(set_key), 10, '0') as set_key, set_name, capacity, availability, goal from structured_data_inc -- order by capacity
;

select set_key_is, set_name, capacity, availability, goal from structured_data_base;
call GetItemsFromSD(
1,10);

select lpad(bin(set_key), 10, '0') as set_key, set_name, capacity, availability, goal from unions_next_rank; -- order by capacity

select lpad(bin(basesets), 10, '0') as set_key, count from raw_inventory;

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


