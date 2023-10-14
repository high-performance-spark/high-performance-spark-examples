#!/bin/bash
# shellcheck disable=SC1091,SC2034

source env_setup.sh

set -ex

set -o pipefail

#tag::package_venv[]
if [ ! -d pyspark_venv ]; then
  python -m venv pyspark_venv
fi

source pyspark_venv/bin/activate
pip install -r ./python/requirements.txt

if [ ! -f pyspark_venv.tar.gz ]; then
  venv-pack -o pyspark_venv.tar.gz
fi


# Set in local and client mode where the driver uses the Python present
# (requires that you have activated the venv as we did above)
PYSPARK_DRIVER_PYTHON=python
export PYSPARK_DRIVER_PYTHON
export PYTHON_PATH=./environment/bin/python
#end::package_venv[]

# Some hack for our json magic
cat se*.json > spark_expectations_sample_rules.json

function check_fail () {
  local ex="$1"
  local code="$2"
  if [ -f "${ex}.fail" ]; then
    echo "ok";
  else
    exit $code
  fi
}

EXAMPLE_JAR="./target/scala-2.12/examples-assembly-0.0.1.jar"

if [ ! -f "${EXAMPLE_JAR}" ]; then
  sbt core/package
fi

function run_example () {
  local ex="$1"
  # shellcheck disable=SC2046
  spark-submit \
	       --master local[5] \
	       --conf spark.eventLog.enabled=true \
	       --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
	       --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
	       --conf spark.sql.catalog.spark_catalog.type=hive \
	       --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
	       --conf spark.sql.catalog.local.type=hadoop \
	       --archives pyspark_venv.tar.gz#environment \
	       --conf "spark.sql.catalog.local.warehouse=$PWD/warehouse" \
	       $(cat "${ex}.conf" || echo "") \
	       --name "${ex}" \
	       --jars "${EXAMPLE_JAR}" \
	       "${ex}" 2>&1 | tee -a "${ex}.out" || check_fail "$ex" $?
}

if [ $# -eq 1 ]; then
  run_example "python/examples/$1"
else
  for ex in python/examples/*.py; do
    run_example "$ex"
  done
fi
