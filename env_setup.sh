#!/bin/bash

set -ex

# Download Spark and iceberg if not present
SPARK_MAJOR=${SPARK_MAJOR:-"3.5"}
SPARK_VERSION=${SPARK_VERSION:-"${SPARK_MAJOR}.1"}
SCALA_VERSION=${SCALA_VERSION:-"2.13"}
HADOOP_VERSION="3"
SPARK_PATH="$(pwd)/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}"
SPARK_FILE="spark-${SPARK_VERSION}-bin-hadoop3.tgz"
if [ "$SCALA_VERSION" = "2.13" ]; then
  SPARK_FILE="spark-${SPARK_VERSION}-bin-hadoop3-scala2.13.tgz"
fi
ICEBERG_VERSION=${ICEBERG_VERSION:-"1.6.0"}
if [ ! -f "${SPARK_FILE}" ]; then
  SPARK_DIST_URL="https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/${SPARK_FILE}"
  SPARK_ARCHIVE_DIST_URL="https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_FILE}"
  if command -v axel &> /dev/null
  then
    (axel "$SPARK_DIST_URL" || axel "$SPARK_ARCHIVE_DIST_URL") &
  else
    (wget "$SPARK_DIST_URL" || wget "$SPARK_ARCHIVE_DIST_URL") &
  fi
fi
# Download Icberg if not present
ICEBERG_FILE="iceberg-spark-runtime-${SPARK_MAJOR}_${SCALA_VERSION}-${ICEBERG_VERSION}.jar"
if [ ! -f "${ICEBERG_FILE}" ]; then
  wget "https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark-runtime-${SPARK_MAJOR}_${SCALA_VERSION}/${ICEBERG_VERSION}/${ICEBERG_FILE}" -O "${ICEBERG_FILE}" &
fi
wait
sleep 1
# Setup the env
if [ ! -d "${SPARK_PATH}" ]; then
  tar -xf "${SPARK_FILE}"
fi

SPARK_HOME="${SPARK_PATH}"
export SPARK_HOME

if [ ! -f "${SPARK_PATH}/jars/${ICEBERG_FILE}" ]; then
  # Delete the old JAR first.
  rm "${SPARK_PATH}/jars/iceberg-spark-runtime*.jar" || echo "No old version to delete."
  cp "${ICEBERG_FILE}" "${SPARK_PATH}/jars/${ICEBERG_FILE}"
fi

# Set up for running pyspark and friends
export PATH="${SPARK_PATH}:${SPARK_PATH}/python:${SPARK_PATH}/bin:${SPARK_PATH}/sbin:${PATH}"

# Make sure we have a history directory
mkdir -p /tmp/spark-events

mkdir -p ./data/fetched/
if [ ! -f ./data/fetched/2021 ]; then
  wget "https://gender-pay-gap.service.gov.uk/viewing/download-data/2021" -O ./data/fetched/2021
fi

