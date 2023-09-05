#!/bin/bash

source env_setup.sh

pip install -r ./python/requirements.txt

for ex in python/examples/*.py; do
  # shellcheck disable=SC2046
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
	       "${ex}" 2>&1 | tee -a "${ex}.out"
done
