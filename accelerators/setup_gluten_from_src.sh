#!/bin/bash
set -ex

# Setup deps
source setup_gluten_deps.sh

# Try gluten w/clickhouse
#if [ ! -d gluten ]; then
#  git clone https://github.com/oap-project/gluten.git
#  cd gluten
#  bash ./ep/build-clickhouse/src/build_clickhouse.sh
#fi

# Build gluten
if [ ! -d gluten ]; then
  # We need Spark 3.5 w/scala212
  git clone git@github.com:holdenk/gluten.git || git clone https://github.com/holdenk/gluten.git
  cd gluten
  git checkout add-spark35-scala213-hack
  ./dev/builddeps-veloxbe.sh
  mvn clean package -Pbackends-velox -Pspark-3.5 -DskipTests
  cd ..
fi
