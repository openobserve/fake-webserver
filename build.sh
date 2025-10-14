#!/bin/bash

VER="$1"
if [ -z "$VER" ]; then
    echo "Usage: $0 <version>"
    exit 1
fi

docker build -t coldstar/fake-webserver:$VER .
docker push coldstar/fake-webserver:$VER
