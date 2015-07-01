unix
wget https://github.com/gbraccialli/DedupTimestampDurationHiveUDAF/raw/master/target/DedupTimeStampDurationHiveUDAF-1.0-SNAPSHOT-jar-with-dependencies.jar
mkdir /usr/hdp/current/hive-server2/auxlib
cp DedupTimeStampDurationHiveUDAF-1.0-SNAPSHOT-jar-with-dependencies.jar /usr/hdp/current/hive-server2/auxlib
--restart hive from ambari

hive or beeline:

CREATE FUNCTION dedup_timestamp_duration as 'com.github.gbraccialli.hive.udaf.DedupTimestampDurationHiveUDAF';
CREATE FUNCTION simple_phonenumber_bucket as 'com.github.gbraccialli.hive.udf.SimplePhoneNumberBucket';
SET hive.map.aggr=false;
