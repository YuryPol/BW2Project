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
 