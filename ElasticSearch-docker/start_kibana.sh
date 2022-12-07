#!/bin/sh

echo "starting up kibana on docker..."

echo "running container 'kib01' on network 'elastic'"
docker run --name kib01 --net elastic -p 127.0.0.1:5601:5601 -e "ELASTICSEARCH_HOSTS=http://es01:9200" docker.elastic.co/kibana/kibana:7.17.7
