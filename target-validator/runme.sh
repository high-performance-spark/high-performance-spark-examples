#!/bin/bash
# shellcheck disable=SC1091,SC2034

source ../env_setup.sh 
set -ex
export SPARK_VERSION="${SPARK_VERSION:-3.4.1}"
git clone git@github.com:holdenk/data-validator.git || git clone https://github.com/holdenk/data-validator.git
cd data-validator
git checkout upgrade-to-modern-spark
sbt -Dspark="${SPARK_VERSION}" clean assembly
JAR_PATH="$(pwd)/target/scala-2.12/data-validator-assembly-${SPARK_VERSION}_0.15.0.jar"
export JAR_PATH
cd ..
spark-submit --master local  "$JAR_PATH" --config ex.yaml || echo "Failed as expected."
