#!/bin/zsh

# if running gremlin server, stop server

# remove db directory
rm -Rf ${JANUSGRAPH_HOME}/db/berkeley/
