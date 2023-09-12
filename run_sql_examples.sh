#!/bin/bash
set -ex
set -o pipefail

source env_setup.sh

# We use `` for mid multi-line command comments. (see https://stackoverflow.com/questions/9522631/how-to-put-a-line-comment-for-a-multi-line-command).
# For each SQL
for sql_file in sql/*.sql; do
  echo "Processing ${sql_file}"
  # shellcheck disable=SC2046
  spark-sql --master local[5] \
	    --conf spark.eventLog.enabled=true \
	    --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
	    --conf spark.sql.catalog.spark_catalog.type=hive \
	    --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
	    --conf spark.sql.catalog.local.type=hadoop \
	    --conf "spark.sql.catalog.local.warehouse=$PWD/warehouse" \
	    $(cat "${sql_file}.conf" || echo "") \
	    --name "${sql_file}" \
	    -f "${sql_file}" | tee -a "${sql_file}.out"
done

# If you want to look at them
# ${SPARK_PATH}/sbin/start-history-server.sh
