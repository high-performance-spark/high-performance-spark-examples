#!/bin/bash
set -ex

# Setup deps
sudo apt-get update && sudo apt-get install -y locales wget tar tzdata git ccache cmake ninja-build build-essential llvm-dev clang libiberty-dev libdwarf-dev libre2-dev libz-dev libssl-dev libboost-all-dev libcurl4-openssl-dev maven rapidjson-dev libdouble-conversion-dev libgflags-dev libsodium-dev libsnappy-dev nasm && sudo apt install -y libunwind-dev && sudo apt-get install -y libgoogle-glog-dev && sudo apt-get -y install docker-compose

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
