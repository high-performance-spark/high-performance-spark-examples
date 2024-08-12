#!/bin/bash
set -ex
./build_container.sh
docker image pull holdenk/hps:0.1
mkdir -p warehouse
mkdir -p iceberg-workshop
docker container  run --mount type=bind,source="$(pwd)"/warehouse,target=/warehouse --mount type=bind,source="$(pwd)/iceberg-workshop",target=/high-performance-spark-examples/iceberg-workshop -p 8877:8877 -it holdenk/hps:0.1 /bin/bash
