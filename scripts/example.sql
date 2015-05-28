
SELECT inline(record) as (date,time,duration,cdr_type,num_a,num_b,central,erb,imsi,emei,other1,other2,other3)
FROM (
  SELECT 
    dedup_timestamp_duration(
      unix_timestamp(concat(date, ' ', time),'yyyyMMdd HHmmss'),duration,cdr_type,struct(date,time,duration,cdr_type,num_a,num_b,central,erb,imsi,emei,other1,other2,other3)
    ) as record 
  FROM cdr_partitioned
  WHERE num_a = '11988556685'
  GROUP BY num_a, num_b
) t1
ORDER BY date, time;



SELECT inline(record) as (date,time,duration,cdr_type,num_a,num_b,central,erb,imsi,emei,other1,other2,other3)
FROM (
  SELECT 
    dedup_timestamp_duration(
      unix_timestamp(concat(date, ' ', time),'yyyyMMdd HHmmss'),duration,cdr_type,struct(date,time,duration,cdr_type,num_a,num_b,central,erb,imsi,emei,other1,other2,other3)
    ) as record 
  FROM cdr_partitioned
  WHERE hash_target = (abs(hash('11981341618'))  % 11) AND
  date BETWEEN 20150102 AND 20150107 AND
  (num_a = '11981341618' or num_b = '11981341618')
  GROUP BY num_a, num_b
) t1
ORDER BY date, time;


SELECT distinct num_a, date
FROM cdr_partitioned
WHERE erb = 'xx'

SELECT /*+ MAPJOIN(erb) */
  cdr.* 
FROM cdr_partitioned cdr
INNER JOIN (
  SELECT distinct num_a, date
  FROM cdr_partitioned
  WHERE erb = 'xx'
) erb ON
  cdr.date = erb.date AND
  cdr.hash_target = (abs(hash(erb.num_a))  % 11)
WHERE
  cdr.direction = 'a-b'
;



SELECT inline(record) as (date,time,duration,cdr_type,num_a,num_b,central,erb,imsi,emei,other1,other2,other3)
FROM (
  SELECT 
    dedup_timestamp_duration(
      unix_timestamp(concat(date, ' ', time),'yyyyMMdd HHmmss'),duration,cdr_type,struct(date,time,duration,cdr_type,num_a,num_b,central,erb,imsi,emei,other1,other2,other3)
    ) as record 
  FROM (
  
      SELECT /*+ MAPJOIN(erb) */
        cdr.* 
      FROM cdr_partitioned cdr
      INNER JOIN (
        SELECT distinct num_a, date
        FROM cdr_partitioned
        WHERE erb = 'xx'
      ) erb ON
        cdr.date = erb.date AND
        cdr.hash_target = (abs(hash(erb.num_a))  % 11)
      WHERE
        cdr.direction = 'a-b'
  
  ) t1
  GROUP BY num_a, num_b
) t2;
