#!/bin/bash
set -ex
VERSION=${VERSION:-0.5}
IMAGE=${IMAGE:-holdenk/hps:$VERSION}
export VERSION
export IMAGE
docker image pull "$IMAGE"
mkdir -p warehouse
mkdir -p iceberg-workshop
docker container  run --mount type=bind,source="$(pwd)"/warehouse,target=/high-performance-spark-examples/warehouse --mount type=bind,source="$(pwd)/iceberg-workshop",target=/high-performance-spark-examples/iceberg-workshop -p 8877:8877 -p 4040:4040 -it "${IMAGE}" # /bin/bash
