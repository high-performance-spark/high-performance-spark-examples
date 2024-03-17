ACCEL_JARS=./
SPARK_MAJOR_VERSION=3.4

if [ ! -f "${GLUTEN_JAR}" ]; then
  wget https://github.com/apache/incubator-gluten/releases/download/v1.1.1/gluten-velox-bundle-spark3.4_2.12-1.1.1.jar
fi
if [ ! -d arrow-datafusion-comet ]; then
  git clone https://github.com/apache/incubator-gluten.git
  cd arrow-datafusion-comet
  make all PROFILES="-Pspark-3.4"
fi
