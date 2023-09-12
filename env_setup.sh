#!/bin/bash


# Download Spark and iceberg if not present
SPARK_MAJOR="3.4"
SPARK_VERSION=3.4.1
HADOOP_VERSION="3"
SPARK_PATH="spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}"
SPARK_FILE="spark-${SPARK_VERSION}-bin-hadoop3.tgz"
ICEBERG_VERSION="1.3.1"
if [ ! -f "${SPARK_FILE}" ]; then
  wget "https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/${SPARK_FILE}" &
fi
# Download Icberg if not present
ICEBERG_FILE="iceberg-spark-runtime-${SPARK_MAJOR}_2.13-${ICEBERG_VERSION}.jar"
if [ ! -f "${ICEBERG_FILE}" ]; then
  wget "https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark-runtime-${SPARK_MAJOR}_2.13/${ICEBERG_VERSION}/${ICEBERG_FILE}" -O "${ICEBERG_FILE}" &
fi
wait
# Setup the env
if [ ! -d "${SPARK_PATH}" ]; then
  tar -xf ${SPARK_FILE}
fi

export SPARK_HOME="${SPARK_PATH}"

if [ ! -f "${SPARK_PATH}/jars/${ICEBERG_FILE}" ]; then
  cp "${ICEBERG_FILE}" "${SPARK_PATH}/jars/${ICEBERG_FILE}"
fi

# Set up for running pyspark and friends
export PATH=${SPARK_PATH}:${SPARK_PATH}/python:${SPARK_PATH}/bin:${SPARK_PATH}/sbin:${PATH}

# Make sure we have a history directory
mkdir -p /tmp/spark-events

mkdir -p ./data/fetched/
if [ ! -f ./data/fetched/2021 ]; then
  wget "https://gender-pay-gap.service.gov.uk/viewing/download-data/2021" -O ./data/fetched/2021
fi
