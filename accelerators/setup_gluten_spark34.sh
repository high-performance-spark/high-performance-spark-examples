#!/bin/bash

mkdir -p /tmp/spark-events
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
ACCEL_JARS=${SCRIPT_DIR}
SPARK_MAJOR_VERSION=3.4
SCALA_VERSION=${SCALA_VERSION:-"2.12"}

set -ex

# Note: this does not work on Ubuntu 23, only on 22
# You might get something like:
# # C  [libgluten.so+0x30c753]  gluten::Runtime::registerFactory(std::string const&, std::function<gluten::Runtime* (std::unordered_map<std::string, std::string, std::hash<std::string>, std::equal_to<std::string>, std::allocator<std::pair<std::string const, std::string> > > const&)>)+0x23


SPARK_VERSION=3.4.2
SPARK_MAJOR=3.4
HADOOP_VERSION=3
SPARK_DIR="spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}"
SPARK_FILE="${SPARK_DIR}.tgz"

export SPARK_MAJOR
export SPARK_VERSION

source setup_gluten_deps.sh

cd ..
source /etc/lsb-release
# Pre-baked only
if [ "$DISTRIB_RELEASE" == "20.04" ]; then
  source ./env_setup.sh
  cd "${SCRIPT_DIR}"

  GLUTEN_JAR="gluten-velox-bundle-spark${SPARK_MAJOR_VERSION}_${SCALA_VERSION}-1.1.0.jar"
  GLUTEN_JAR_PATH="${SCRIPT_DIR}/gluten-velox-bundle-spark${SPARK_MAJOR_VERSION}_${SCALA_VERSION}-1.1.0.jar"

  if [ ! -f "${GLUTEN_JAR_PATH}" ]; then
    wget "https://github.com/oap-project/gluten/releases/download/v1.1.0/${GLUTEN_JAR}" || unset GLUTEN_JAR_PATH
  fi

fi
# Rather than if/else we fall through to build if wget fails because major version is not supported.
if [ -z "$GLUTEN_JAR_PATH" ]; then
  if [ ! -d vcpkg ]; then
    git clone https://github.com/microsoft/vcpkg
  fi
  cd vcpkg
  ./vcpkg/bootstrap-vcpkg.sh
  if [ ! -d incubator-gluten ]; then
    git clone https://github.com/apache/incubator-gluten.git
  fi
  cd incubator-gluten
  sudo ./dev/builddeps-veloxbe.sh --enable_s3=ON --enable_vcpkg=ON
  mvn clean package -Pbackends-velox -Pspark-3.4 -DskipTests
  GLUTEN_JAR_PATH="$(pwd)/package/target/gluten-package-*-SNAPSHOT-${SPARK_MAJOR_VERSION}.jar"
fi

export GLUTEN_JAR_PATH

