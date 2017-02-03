LOCK TABLE structured_data_base AS sdbW WRITE, structured_data_base AS sdbR READ, raw_inventory AS ri READ;

UPDATE structured_data_base AS sdbW,
  (SELECT set_key, SUM(ri.count) AS capacity, SUM(ri.count) AS availability
   FROM structured_data_base AS sdbR
   JOIN raw_inventory AS ri
   ON set_key & ri.basesets != 0
   GROUP BY set_key) comp
 SET sdbW.capacity = comp.capacity,
     sdbW.availability = comp.availability
 WHERE sdbW.set_key = comp.set_key;

SELECT basesets, criteria, cnt from
(
	SELECT basesets, raw_inventory.criteria, count(*) as cnt
	FROM structured_data_base
	JOIN raw_inventory
	ON set_key & basesets != 0
	GROUP BY basesets
) tmp
WHERE cnt < 4;

select * from structured_data_inc
join raw_inventory
where (structured_data_inc.set_key & raw_inventory.basesets
  =    structured_data_inc.set_key | raw_inventory.basesets 
AND    structured_data_inc.set_key & raw_inventory.basesets > 1);

SELECT * FROM (
SELECT structured_data_base.set_key_is | unions_last_rank.set_key as k, MIN(BIT_COUNT(structured_data_base.set_key_is & unions_last_rank.set_key & raw_inventory.basesets)) as c
FROM  unions_last_rank
JOIN  structured_data_base
JOIN  raw_inventory
ON structured_data_base.set_key_is & raw_inventory.basesets != 0
    AND unions_last_rank.set_key & raw_inventory.basesets != 0 
    AND (structured_data_base.set_key_is | unions_last_rank.set_key != unions_last_rank.set_key 
    OR BIT_COUNT(unions_last_rank.set_key) = 1)
GROUP BY structured_data_base.set_key_is | unions_last_rank.set_key) tmp
WHERE c = 1;

SELECT structured_data_base.set_key_is | unions_last_rank.set_key as k, BIT_COUNT(structured_data_base.set_key_is & unions_last_rank.set_key & raw_inventory.basesets) as c
FROM  unions_last_rank
JOIN  structured_data_base
JOIN  raw_inventory
ON structured_data_base.set_key_is & raw_inventory.basesets != 0                               
    AND unions_last_rank.set_key & raw_inventory.basesets != 0 
    AND structured_data_base.set_key_is | unions_last_rank.set_key != unions_last_rank.set_key;
    
-- AddUnionsDynamic() 
-- INSERT /*IGNORE*/ INTO unions_next_rank
   SELECT structured_data_base.set_key_is | unions_last_rank.set_key, NULL, NULL, NULL, 0
   FROM unions_last_rank
   JOIN structured_data_base
   JOIN raw_inventory
       ON  (structured_data_base.set_key_is & raw_inventory.basesets != 0)
       AND (unions_last_rank.set_key & raw_inventory.basesets) != 0
       AND (structured_data_base.set_key_is | unions_last_rank.set_key) != unions_last_rank.set_key
       AND BIT_COUNT(structured_data_base.set_key_is | unions_last_rank.set_key) > BIT_COUNT(structured_data_base.set_key_is)
   GROUP BY structured_data_base.set_key_is | unions_last_rank.set_key;

   SELECT structured_data_base.set_key_is | unions_last_rank.set_key, NULL, NULL, NULL, 0
   FROM unions_last_rank
   JOIN structured_data_base
   JOIN raw_inventory
       ON  (structured_data_base.set_key_is & raw_inventory.basesets != 0)
       AND (unions_last_rank.set_key & raw_inventory.basesets) != 0
       AND (structured_data_base.set_key_is | unions_last_rank.set_key) != unions_last_rank.set_key
       AND structured_data_base.set_key_is | unions_last_rank.set_key > structured_data_base.set_key_is
   GROUP BY structured_data_base.set_key_is | unions_last_rank.set_key;
   
   SELECT structured_data_base.set_key_is | unions_last_rank.set_key, NULL, NULL, NULL, 0
   FROM unions_last_rank
   JOIN structured_data_base
   JOIN raw_inventory
       ON  (structured_data_base.set_key_is & raw_inventory.basesets != 0)
       AND (unions_last_rank.set_key & raw_inventory.basesets) != 0
       AND (structured_data_base.set_key_is | unions_last_rank.set_key) != unions_last_rank.set_key
       AND structured_data_base.set_key_is | unions_last_rank.set_key > structured_data_base.set_key_is
   GROUP BY structured_data_base.set_key_is | unions_last_rank.set_key;
   
   
   
   
   
   
   
   