#!/bin/bash

set -ex
source install_rust_if_needed.sh

if [ ! -d arrow-datafusion-comet ]; then
  git clone https://github.com/apache/arrow-datafusion-comet.git
fi

if [ -z "$(ls arrow-datafusion-comet/spark/target/comet-spark-spark*.jar)" ]; then
  cd arrow-datafusion-comet
  make clean release PROFILES="-Pspark-${SPARK_MAJOR}"
fi
COMET_JAR="$(pwd)/$(ls arrow-datafusion-comet/spark/target/comet-spark-spark*SNAPSHOT.jar)"
export COMET_JAR
