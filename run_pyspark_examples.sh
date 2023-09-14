#!/bin/bash

source env_setup.sh

set -ex

set -o pipefail

if [ ! -d pyspark_venv ]; then
  python -m venv pyspark_venv
fi

source pyspark_venv/bin/activate
pip install -r ./python/requirements.txt

if [ ! -f pyspark_venv.tar.gz ]; then
  venv-pack -o pyspark_venv.tar.gz
fi


export PYSPARK_PYTHON=$(which python)
export PYSPARK_DRIVER_PYTHON=$(which python)
export PYTHON_PATH=./environment/bin/python

function run_example () {
  local ex="$1"
  # shellcheck disable=SC2046
  spark-submit \
	       --master local[5] \
	       --conf spark.eventLog.enabled=true \
	       --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
	       --conf spark.sql.catalog.spark_catalog.type=hive \
	       --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
	       --conf spark.sql.catalog.local.type=hadoop \
	       --conf spark.pyspark.driver.python=${PYSPARK_DRIVER_PYTHON} \
	       --conf spark.pyspark.driver.python=${PYSPARK_PYTHON} \
	       --archives pyspark_venv.tar.gz#environment \
	       --conf "spark.sql.catalog.local.warehouse=$PWD/warehouse" \
	       $(cat "${ex}.conf" || echo "") \
	       --name "${ex}" \
	       "${ex}" 2>&1 | tee -a "${ex}.out" || echo "ok"
}


if [ $# -eq 1 ]; then
  run_example "python/examples/$1"
else
  for ex in python/examples/*.py; do
    run_example "$ex"
  done
fi
