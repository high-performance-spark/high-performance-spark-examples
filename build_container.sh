#!/bin/bash

set -ex

cp .git/index /tmp/git_index
export GIT_INDEX_FILE=/tmp/git_index
git add -u
hash=$(git write-tree)
unset GIT_INDEX_FILE
oldhash=$(cat oldhash || true)
if [ "$hash" = "$oldhash" ] && [ -f myapp.tar ]; then
  echo "Skipping making tar since we match."
else
  echo "Making tar since no match"
  git archive -o myapp.tar --format=tar HEAD
  echo "$hash" > oldhash
fi
VERSION=${VERSION:-0.3}
IMAGE=${IMAGE:-holdenk/hps:$VERSION}
MINI_IMAGE=${MINI_IMAGE:-holdenk/hps-mini:$VERSION}
docker buildx build --platform=linux/amd64,linux/arm64 -t "${IMAGE}" .  --push
docker buildx build --platform=linux/amd64,linux/arm64 -t "${MINI_IMAGE}" -f Dockerfile-mini .  --push
#docker buildx build --platform=linux/amd64 -t "${IMAGE}" .  --push
