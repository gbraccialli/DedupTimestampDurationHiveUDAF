DROP TABLE cdr_raw;
CREATE EXTERNAL TABLE cdr_raw (
date string,
time string,
duration int,
cdr_type string,
num_a string,
num_b string,
central string,
erb string,
imsi string,
emei string,
other1 string,
other2 string,
other3 string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/test/cdr/';




DROP TABLE cdr_partitioned;
CREATE TABLE cdr_partitioned (
time string,
duration int,
cdr_type string,
num_a string,
num_b string,
central string,
erb string,
imsi string,
emei string,
other1 string,
other2 string,
other3 string
)
PARTITIONED BY(date STRING, direction string, hash_target int)
stored as orc;



FROM cdr_raw 
INSERT INTO TABLE cdr_partitioned PARTITION(date, direction, hash_target)
SELECT
  time, duration, cdr_type, num_a, num_b, central, erb, imsi, emei, other1, other2, other3, 
  date, 'a-b', simple_phonenumber_bucket(num_a,5) as hash_target
SORT BY erb, emei
INSERT INTO TABLE cdr_partitioned PARTITION(date, direction, hash_target)
SELECT
  time, duration, cdr_type, num_a, num_b, central, erb, imsi, emei, other1, other2, other3, 
  date, 'b-a', simple_phonenumber_bucket(num_b,5) as hash_target
SORT BY erb, emei  
;


