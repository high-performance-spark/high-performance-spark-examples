#!/bin/bash

set -ex

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cd "${SCRIPT_DIR}"
source "${SCRIPT_DIR}/setup_gluten_spark34.sh"

export SPARK_HOME
PATH="$(pwd)/${SPARK_DIR}/bin:$PATH"
export PATH
"${SPARK_HOME}/bin/spark-sql" --master local[5] \
  --conf spark.plugins=io.glutenproject.GlutenPlugin \
  --conf spark.memory.offHeap.enabled=true \
  --conf spark.memory.offHeap.size=5g \
  --jars "${GLUTEN_JAR}" \
  --conf spark.eventLog.enabled=true \
  -e "SELECT 1"

source gluten_env_setup.sh
cd ..
./run_sql_examples.sh || echo "Expected to fail"
