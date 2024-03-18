#!/bin/bash

set -ex

source install_rust_if_needed.sh

if [ ! -d arrow-datafusion-comet ]; then
  git clone https://github.com/apache/arrow-datafusion-comet.git
fi

if [ -z $(ls arrow-datafusion-comet/spark/target/comet-spark-spark*.jar) ]; then
  cd arrow-datafusion-comet
  make clean release PROFILES="-Pspark-3.4"
fi
COMET_JAR="$(pwd)/$(ls incubator-comet/spark/target/comet-spark-spark*.jar)"
export COMET_JAR
