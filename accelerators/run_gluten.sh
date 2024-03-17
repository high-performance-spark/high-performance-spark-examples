${SPARK_HOME}/bin/spark-shell --master local --jars ${ACCEL_JARS}/gluten-velox-bundle-spark${SPARK_MAJOR_VERSION}_2.12-1.1.1.jar  --spark-properties=gluten_config.properties
