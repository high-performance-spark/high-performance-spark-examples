#!/bin/bash
set -ex
./build_container.sh
docker image pull holdenk/hps:0.1
docker container  run --mount type=bind,source="$(pwd)"/warehouse,target=/warehouse -p 8888:8888 -it holdenk/hps:0.1 /bin/bash
