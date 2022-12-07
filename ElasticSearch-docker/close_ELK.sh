#!/bin/sh

echo "stopping containers 'es01' and 'kib01'"
docker stop es01
docker stop kib01

echo "removing network 'elastic', and containers 'es01' and 'kib01'"
docker network rm elastic
docker rm es01
docker rm kib01