#!/bin/bash

YARN_CONF="$YARN_CONF --conf spark.serializer=org.apache.spark.serializer.KryoSerializer"
YARN_CONF="$YARN_CONF --conf spark.kryoserializer.buffer.max=68m"
YARN_CONF="$YARN_CONF --conf spark.driver.memory=40g"
YARN_CONF="$YARN_CONF --conf spark.executor.memory=18g" 
YARN_CONF="$YARN_CONF --conf spark.executor.cores=5" 

busin=$1
arg1=amazon
arg2=$2

if [ "$busin" == "sears" ]; then
	divname="spark_sears_safety_stock.R"
else
	divname="spark_kmart_safety_stock.R"
fi
echo $arg1
echo $arg2
echo $busin
echo $divname
TIME=$(date +%k%M)
DAY=`/bin/date +%Y%m%d`
echo 'todays date'
echo $DAY
cloud_LOCATION="s3://shc-afe-dev.appspot.com/optimal_inventory/$busin/$DAY/"
echo $GS_LOCATION

spark-submit $YARN_CONF --packages com.databricks:spark-csv_2.10:1.4.0 --master yarn $divname $arg1 $arg2

echo "start hdfs merge"
hdfs dfs -getmerge /user/root/optimal-inventory.csv optimal-inventory.csv
echo "end hdfs merge"
echo "start Zipping the file"
gzip optimal-inventory.csv optimal-inventory.csv.gz
echo "end Zipping the file"
echo "end hdfs merge"
echo "start google copy"
gsutil cp optimal-inventory.csv.gz $cloud_LOCATION
echo "end google copy"