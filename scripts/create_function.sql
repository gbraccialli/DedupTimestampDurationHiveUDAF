--wget https://github.com/gbraccialli/GeohashHiveUDF/raw/master/target/GeohashHiveUDF-1.0-SNAPSHOT-jar-with-dependencies.jar
ADD JAR /jars/DedupTimeStampDurationHiveUDAF-1.0-SNAPSHOT-jar-with-dependencies.jar;
CREATE TEMPORARY FUNCTION dedup_timestamp_duration as 'com.github.gbraccialli.hive.udaf.DedupTimestampDurationHiveUDAF';
SET hive.map.aggr=false;
