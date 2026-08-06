#!/bin/bash

VER="$1"
if [ -z "$VER" ]; then
    echo "Usage: $0 <version>"
    exit 1
fi

docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -t openobserve/fake-webserver:$VER \
  --push \
  .
