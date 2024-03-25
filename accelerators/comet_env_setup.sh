#!/bin/bash

SPARK_EXTRA="
--jars ${COMET_JAR} \
--driver-class-path ${COMET_JAR} \
--conf spark.comet.enabled=true \
--conf spark.comet.exec.enabled=true \
--conf spark.comet.exec.all.enabled=true \
--conf spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager \
--conf spark.comet.exec.shuffle.enabled=true \
--conf spark.comet.columnar.shuffle.enabled=true"
# Instead of --conf spark.sql.extensions=org.apache.comet.CometSparkSessionExtensions we set
# EXTRA_EXTENSIONS so it can be appended to iceberg
if [ -z "$EXTRA_EXTENSIONS" ]; then
  EXTRA_EXTENSIONS="org.apache.comet.CometSparkSessionExtensions"
else
  EXTRA_EXTENSIONS="org.apache.comet.CometSparkSessionExtensions,$EXTRA_EXTENSIONS"
fi
export EXTRA_EXTENSIONS
export SPARK_EXTRA
