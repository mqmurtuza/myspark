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
dc=$3

if  [ "$busin" == "sears" ] && [ "$dc" == "dc" ]; then
	filename="spark_sears_safety_stock_DC.R"
elif [ "$busin" == "kmart" ] && [ "$dc" == "dc" ]; then
	filename="spark_kmart_safety_stock_DC.R"
elif [ "$busin" == "sears" ]; then
	filename="spark_sears_safety_stock.R"
elif [ "$busin" == "kmart" ]; then
	filename="spark_kmart_safety_stock.R"	
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
if  [ "$dc" == "dc" ];  then
hdfs dfs -getmerge /user/root/optimal-inventory-$busin-dc.csv optimal-inventory-$busin-dc.csv
else
hdfs dfs -getmerge /user/root/optimal-inventory-$busin.csv optimal-inventory-$busin.csv
fi
echo "end hdfs merge"
echo "start Zipping the file"
if  [ "$dc" == "dc" ];  then
gzip optimal-inventory-$busin-dc.csv optimal-inventory-$busin-dc.csv.gz
else
gzip optimal-inventory-$busin.csv optimal-inventory-$busin.csv.gz
fi
echo "end Zipping the file"
echo "start google copy"
if  [ "$dc" == "dc" ];  then
gsutil cp optimal-inventory-$busin-dc.csv.gz $GS_LOCATION
else
gsutil cp optimal-inventory-$busin.csv.gz $GS_LOCATION
fi
echo "end google copy"