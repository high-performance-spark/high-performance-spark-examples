#!/bin/bash

set -ex

cp .git/index /tmp/git_index
export GIT_INDEX_FILE=/tmp/git_index
git add -u
hash=$(git write-tree)
unset GIT_INDEX_FILE
oldhash=$(cat oldhash || "")
if [ "$hash" = "$oldhash" -a -f myapp.tar ]; then
  echo "Skipping making tar since we match."
else
  echo "Making tar since no match"
  git archive -o myapp.tar --format=tar HEAD
  echo "$hash" > oldhash
fi
IMAGE=holdenk/hps:0.1
docker buildx build --platform=linux/amd64,linux/arm64 -t "${IMAGE}" .  --push
#docker buildx build --platform=linux/amd64 -t "${IMAGE}" .  --push
