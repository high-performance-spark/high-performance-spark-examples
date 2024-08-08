#!/bin/bash

if [ -f myapp.tar ]; then
  rm myapp.tar
fi
git archive -v -o myapp.tar --format=tar HEAD
IMAGE=holdenk/hps:0.1
docker buildx build --platform=linux/amd64,linux/arm64 -t "${IMAGE}" .  --push
