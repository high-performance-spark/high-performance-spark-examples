#!/bin/bash

source env_setup.sh

for ex in python/examples/*.py; do
  spark-submit \
	       --master local[5] \
	       --conf spark.eventLog.enabled=true \
	       --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
	       --conf spark.sql.catalog.spark_catalog.type=hive \
	       --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
	       --conf spark.sql.catalog.local.type=hadoop \
	       --conf "spark.sql.catalog.local.warehouse=$PWD/warehouse" \
	       $(cat "${ex}.conf" || echo "") \
	       --name "${ex}" \
	       ${ex} | tee -a "${ex}.out"
done
