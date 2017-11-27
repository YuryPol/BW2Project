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
   
   
SELECT DISTINCT unions_last_rank.set_key  FROM unions_last_rank 
 LEFT OUTER JOIN unions_next_rank
 ON unions_last_rank.set_key & unions_next_rank.set_key = unions_last_rank.set_key
 AND   unions_last_rank.capacity = unions_next_rank.capacity;
 
   
 SELECT 
    set_key, 
    NULL as set_name, 
    SUM(capacity) as capacity, 
    SUM(availability) as availability, 
    0 as goal 
 FROM (
  SELECT *, raw_inventory.count as capacity, raw_inventory.count as availability 
  FROM (
    SELECT DISTINCT structured_data_base.set_key_is | unions_last_rank.set_key as set_key 
       FROM unions_last_rank    JOIN structured_data_base      JOIN raw_inventory         ON  structured_data_base.set_key_is & raw_inventory.basesets != 0 
         AND unions_last_rank.set_key & raw_inventory.basesets != 0 
         AND structured_data_base.set_key_is | unions_last_rank.set_key > unions_last_rank.set_key 
   ) un_sk 
   JOIN raw_inventory   ON un_sk.set_key & raw_inventory.basesets != 0 
 ) un_r
 GROUP BY set_key
 
SELECT  
lpad(bin(unions_last_rank.set_key), 20, '0'), lpad(bin(unions_next_rank.set_key), 20, '0')  
FROM unions_last_rank
JOIN unions_next_rank
      ON unions_last_rank.set_key & unions_next_rank.set_key = unions_last_rank.set_key 
      AND unions_last_rank.capacity = unions_next_rank.capacity 
      AND unions_next_rank.set_key IS NOT NULL
;

 -- INSERT /*IGNORE*/ INTO unions_next_rank 
 SELECT 
    set_key, 
    NULL as set_name, 
    SUM(capacity) as capacity, 
    SUM(availability) as availability, 
    0 as goal 
 FROM (
  SELECT *, raw_inventory.count as capacity, raw_inventory.count as availability 
  FROM (
    SELECT DISTINCT structured_data_base.set_key_is | unions_last_rank.set_key as set_key 
       FROM unions_last_rank    
       JOIN structured_data_base      
       JOIN raw_inventory         
       ON  structured_data_base.set_key_is & raw_inventory.basesets != 0 
         AND unions_last_rank.set_key & raw_inventory.basesets != 0 
         AND structured_data_base.set_key_is | unions_last_rank.set_key > unions_last_rank.set_key 
   ) un_sk 
   JOIN raw_inventory   
   ON un_sk.set_key & raw_inventory.basesets != 0 
   AND un_sk.set_key IN(null,0) 
 ) un_r 
 GROUP BY set_key

 INSERT /*IGNORE*/ INTO unions_next_rank SELECT 
    set_key, 
    NULL as set_name, 
    SUM(capacity) as capacity, 
    SUM(availability) as availability, 
    0 as goal 
 FROM (
  SELECT *, raw_inventory.count as capacity, raw_inventory.count - structured_data_base.goal as availability 
  FROM (
    SELECT DISTINCT structured_data_base.set_key_is | unions_last_rank.set_key as set_key 
       FROM unions_last_rank    JOIN structured_data_base      JOIN raw_inventory         ON  structured_data_base.set_key_is & raw_inventory.basesets != 0 
         AND unions_last_rank.set_key & raw_inventory.basesets != 0 
         AND structured_data_base.set_key_is | unions_last_rank.set_key > unions_last_rank.set_key 
   ) un_sk 
   JOIN raw_inventory   ON un_sk.set_key & raw_inventory.basesets != 0 
 ) un_r
 GROUP BY set_key
 
INSERT /*IGNORE*/ INTO unions_next_rank
SELECT un.set_key, NULL AS set_name, un.capacity, un.capacity - SUM(structured_data_base.goal) AS availability,
SUM(structured_data_base.goal) as goal
FROM (
 SELECT 
    set_key, 
    SUM(capacity) as capacity 
 FROM (
  SELECT *, raw_inventory.count as capacity   FROM (
    SELECT DISTINCT structured_data_base.set_key_is | unions_last_rank.set_key as set_key 
       FROM unions_last_rank
    JOIN structured_data_base
       JOIN raw_inventory
         ON  structured_data_base.set_key_is & raw_inventory.basesets != 0 
         AND unions_last_rank.set_key & raw_inventory.basesets != 0 
         AND structured_data_base.set_key_is | unions_last_rank.set_key > unions_last_rank.set_key 
   ) un_sk 
   JOIN raw_inventory   ON un_sk.set_key & raw_inventory.basesets != 0 
 ) un_r
 GROUP BY set_key) un
JOIN structured_data_base 
ON structured_data_base.set_key & un.set_key != 0 
GROUP BY structured_data_base.set_key 
;

INSERT IGNORE INTO unions_next_rank
SELECT un.set_key, NULL AS set_name, un.capacity, un.capacity - SUM(structured_data_base.goal) AS availability,
SUM(structured_data_base.goal) as goal
FROM (
 SELECT 
    set_key, 
    SUM(capacity) as capacity 
 FROM (
  SELECT *, raw_inventory.count as capacity   FROM (
    SELECT DISTINCT structured_data_base.set_key_is | unions_last_rank.set_key as set_key 
       FROM unions_last_rank
    JOIN structured_data_base
       JOIN raw_inventory
         ON  structured_data_base.set_key_is & raw_inventory.basesets != 0 
         AND unions_last_rank.set_key & raw_inventory.basesets != 0 
         AND structured_data_base.set_key_is | unions_last_rank.set_key > unions_last_rank.set_key 
   ) un_sk 
   JOIN raw_inventory   ON un_sk.set_key & raw_inventory.basesets != 0 
 ) un_r
 GROUP BY set_key)  
 un
JOIN structured_data_base 
ON structured_data_base.set_key & un.set_key != 0 
GROUP BY un.set_key 
