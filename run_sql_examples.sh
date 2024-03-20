#!/bin/bash
set -ex
set -o pipefail

source env_setup.sh

function run_example () {
  local sql_file="$1"
  local extra="$2"
  EXTENSIONS=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
  if [ ! -z "$EXTRA_EXTENSIONS" ]; then
    EXTENSIONS="$EXTENSIONS,$EXTRA_EXTENSIONS"
  fi
  # shellcheck disable=SC2046,SC2086
  ${SPARK_HOME}/bin/spark-sql --master local[5] \
	    --conf spark.eventLog.enabled=true \
	    --conf spark.sql.extensions=$EXTENSIONS \
	    --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
	    --conf spark.sql.catalog.spark_catalog.type=hive \
	    --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
	    --conf spark.sql.catalog.local.type=hadoop \
	    --conf "spark.sql.catalog.local.warehouse=$PWD/warehouse" \
	    ${extra} ${SPARK_EXTRA} \
	    $(cat "${sql_file}.conf" || echo "") \
	    --name "${sql_file}" \
	    -f "${sql_file}" | tee -a "${sql_file}.out" || ls "${sql_file}.expected_to_fail"
}


# If you want to look at them
# ${SPARK_PATH}/sbin/start-history-server.sh

if [ $# -eq 1 ]; then
  if [[ "$1" != *"gluten_only"* ]]; then
    run_example "sql/$1"
  else
    echo "Processing gluten ${sql_file}"
    # shellcheck disable=SC2046
    run_example "$sql_file"
  fi
else
  # For each SQL
  for sql_file in sql/*.sql; do
    if [[ "$sql_file" != *"_only"* ]]; then
      echo "Processing ${sql_file}"
      # shellcheck disable=SC2046
      run_example "$sql_file"
    elif [[ "$sql_file" != *"gluten_only"* && "$GLUTEN_EXISTS" == "true" ]]; then
      echo "Processing gluten ${sql_file}"
      # shellcheck disable=SC2046
      run_example "$sql_file"
    elif [[ "$sql_file" != *"gluten_udf_only"* && "$GLUTEN_UDF_EXISTS" == "true" ]]; then
      echo "Processing gluten UDF ${sql_file}"
      # shellcheck disable=SC2046
      run_example "$sql_file"
    else
      echo "Skipping $sql_file since we did not find gluten and this is restricted example."
    fi
  done
fi
