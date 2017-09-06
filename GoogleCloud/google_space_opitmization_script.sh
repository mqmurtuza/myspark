#!/bin/bash

YARN_CONF="$YARN_CONF --conf spark.serializer=org.apache.spark.serializer.KryoSerializer"
YARN_CONF="$YARN_CONF --conf spark.kryoserializer.buffer.max=68m"
YARN_CONF="$YARN_CONF --conf spark.dynamicAllocation.enabled=true"
YARN_CONF="$YARN_CONF --conf spark.shuffle.service.enabled=true"
#YARN_CONF="$YARN_CONF --conf spark.executor.memory=18g" 
#YARN_CONF="$YARN_CONF --conf spark.executor.cores=5" 

busin=$1
env=google
inputLocation=$2

if [ "$busin" == "sears" ]; then
	filename="spark_sears_space_optimization.R"
else
	filename="kmart_space_optimization_52.R"
fi
echo $env
echo $inputLocation
echo $busin
echo $filename
TIME=$(date +%k%M)
DAY=`/bin/date +%Y%m%d`
echo $DAY
GS_LOCATION="gs://shc-afe-dev.appspot.com/optimal_inventory/$busin/$DAY/"
echo $GS_LOCATION

spark-submit $YARN_CONF --packages com.databricks:spark-csv_2.10:1.4.0 --master yarn --driver-memory 60g $filename $env $inputLocation

echo "start hdfs merge"
hdfs dfs -getmerge /user/root/optimal-space-optimization-$busin.csv optimal-space-optimization-$busin.csv
echo "end hdfs merge"
echo "start Zipping the file"
gzip optimal-space-optimization-$busin.csv optimal-space-optimization-$busin.csv.gz
echo "end Zipping the file"
echo "start google copy"
gsutil cp optimal-space-optimization-$busin.csv.gz $GS_LOCATION
echo "end google copy"
