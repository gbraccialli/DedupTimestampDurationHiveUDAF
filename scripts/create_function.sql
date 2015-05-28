--wget https://github.com/gbraccialli/DedupTimestampDurationHiveUDAF/raw/master/target/DedupTimeStampDurationHiveUDAF-1.0-SNAPSHOT-jar-with-dependencies.jar
ADD JAR /jars/DedupTimeStampDurationHiveUDAF-1.0-SNAPSHOT-jar-with-dependencies.jar;
CREATE TEMPORARY FUNCTION dedup_timestamp_duration as 'com.github.gbraccialli.hive.udaf.DedupTimestampDurationHiveUDAF';
SET hive.map.aggr=false;


OR

CREATE TEMPORARY FUNCTION dedup_timestamp_duration AS 'com.github.gbraccialli.hive.udaf.DedupTimestampDurationHiveUDAF' USING JAR 'hdfs:///jars/DedupTimeStampDurationHiveUDAF-1.0-SNAPSHOT-jar-with-dependencies.jar'
SET hive.map.aggr=false;


