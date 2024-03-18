#!/bin/bash

SPARK_EXTRA="--jars ${COMET_JAR} \
--conf spark.sql.extensions=org.apache.comet.CometSparkSessionExtensions \
--conf spark.comet.enabled=true \
--conf spark.comet.exec.enabled=true \
--conf spark.comet.exec.all.enabled=true"
export SPARK_EXTRA
