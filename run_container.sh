#!/bin/bash
set -ex
IMAGE=${IMAGE:-holdenk/hps:0.2}
export IMAGE
./build_container.sh
docker image pull $IMAGE
mkdir -p warehouse
mkdir -p iceberg-workshop
docker container  run --mount type=bind,source="$(pwd)"/warehouse,target=/high-performance-spark-examples/warehouse --mount type=bind,source="$(pwd)/iceberg-workshop",target=/high-performance-spark-examples/iceberg-workshop -p 8877:8877 -p 4040:4040 -it ${IMAGE} # /bin/bash
