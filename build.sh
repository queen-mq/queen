#!/bin/bash
set -e

REGISTRY=$1
NAME=$(cat server/server.json | jq -r '.name')
TAG=$(cat server/server.json | jq -r '.version')
IMAGE=$REGISTRY/$NAME:$TAG

docker build . --platform linux/amd64 --build-arg QUEENCTL_VERSION=$TAG -t $IMAGE
docker push $IMAGE