#!/bin/sh

echo "starting up elastic_search on docker..."

echo "creating network 'elastic'"
docker network create elastic
echo "running container 'es01' on network 'elastic'"
docker run --name es01 --net elastic -p 127.0.0.1:9200:9200 -p 127.0.0.1:9300:9300 -e "discovery.type=single-node" docker.elastic.co/elasticsearch/elasticsearch:7.17.7

