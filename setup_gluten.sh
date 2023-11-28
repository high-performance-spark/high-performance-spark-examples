#!/bin/bash
set -ex

# Setup deps
sudo apt-get update && sudo apt-get install -y locales wget tar tzdata git ccache cmake ninja-build build-essential llvm-dev clang libiberty-dev libdwarf-dev libre2-dev libz-dev libssl-dev libboost-all-dev libcurl4-openssl-dev maven rapidjson-dev libdouble-conversion-dev libgflags-dev libsodium-dev libsnappy-dev && sudo apt install -y libunwind-dev && sudo apt-get install -y libgoogle-glog-dev

# Build gluten
if [ ! -d gluten ]; then
  # We need Spark 3.5 w/scala212
  git clone git@github.com:holdenk/gluten.git || git clone https://github.com/holdenk/gluten.git
  cd gluten
  git checkout add-spark35-scala213
  ./dev/builddeps-veloxbe.sh --build_tests=ON --build_benchmarks=ON --enable_s3=ON  --enable_hdfs=ON 
  mvn clean package -Pbackends-velox -Pspark-3.5 -DskipTests
  cd ..
fi
