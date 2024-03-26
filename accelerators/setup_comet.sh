#!/bin/bash

set -ex
source install_rust_if_needed.sh

if [ -z "${SPARK_MAJOR}" ]; then
  echo "Need a spark major version specified."
  exit 1
else
  echo "Building comet for Spark ${SPARK_MAJOR}"
fi

#tag::build[]
# If we don't have fusion checked out do it
if [ ! -d arrow-datafusion-comet ]; then
  git clone https://github.com/apache/arrow-datafusion-comet.git
fi

# Build JAR if not present
if [ -z "$(ls arrow-datafusion-comet/spark/target/comet-spark-spark*.jar)" ]; then
  cd arrow-datafusion-comet
  make clean release PROFILES="-Pspark-${SPARK_MAJOR}"
  cd ..
fi
COMET_JAR="$(pwd)/$(ls arrow-datafusion-comet/spark/target/comet-spark-spark*SNAPSHOT.jar)"
export COMET_JAR
#end::build[]
