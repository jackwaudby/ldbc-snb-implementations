#!/usr/bin/env bash

echo "JANUSGRAPH_HOME: $JANUSGRAPH_HOME"

$JANUSGRAPH_HOME/bin/gremlin-server.sh  $JANUSGRAPH_HOME/conf/gremlin-server/gremlin-server-berkeleyje.yaml
