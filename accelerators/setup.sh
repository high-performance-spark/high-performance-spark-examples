ACCEL_JARS=./
SPARK_MAJOR_VERSION=3.4

set -ex

# Note: this does not work on Ubuntu 23, only on 22
# You might get something like:
# # C  [libgluten.so+0x30c753]  gluten::Runtime::registerFactory(std::string const&, std::function<gluten::Runtime* (std::unordered_map<std::string, std::string, std::hash<std::string>, std::equal_to<std::string>, std::allocator<std::pair<std::string const, std::string> > > const&)>)+0x23


SPARK_VERSION=3.4.2
HADOOP_VERSION=3
SPARK_DIR=spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}
SPARK_FILE=${SPARK_DIR}.tgz

if [ ! -d ${SPARK_DIR} ]; then
  wget https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/${SPARK_FILE}
  tar -xvf ${SPARK_FILE}
fi

GLUTEN_JAR=gluten-velox-bundle-spark3.4_2.12-1.1.0.jar

if [ ! -f ${GLUTEN_JAR} ]; then
  wget https://github.com/oap-project/gluten/releases/download/v1.1.0/${GLUTEN_JAR}
fi

if [ ! -f "${GLUTEN_JAR}" ]; then
  wget https://github.com/apache/incubator-gluten/releases/download/v1.1.1/gluten-velox-bundle-spark3.4_2.12-1.1.1.jar
fi
if [ ! -d arrow-datafusion-comet ]; then
  git clone https://github.com/apache/incubator-gluten.git
  cd arrow-datafusion-comet
  make all PROFILES="-Pspark-3.4"
fi
