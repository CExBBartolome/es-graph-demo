# i-Sight Graph Demo

## Requirements
- Docker environment with 4096MB of RAM
- `docker-compose` cli

## Setup
```
git clone https://github.com/CExBBartolome/isight-graph.git

cd ./isight-graph
docker-compose up -d

export DOCKER_IP=`echo $DOCKER_HOST | awk -F/ '{print $3}' | awk -F: '{print $1}'`
open http://${DOCKER_IP}:5601
```

## Add'l Instructions

There's a Python `script` container that could be used to populate the index.
