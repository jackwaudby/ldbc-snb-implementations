#!/usr/bin/env bash

echo "loading validation data in DATAGEN directory"

# restruct directory if needed
#mv ${LDBC_SNB_DATAGEN_HOME}/social_network/dynamic/*.csv ${LDBC_SNB_DATAGEN_HOME}/social_network/
#mv ${LDBC_SNB_DATAGEN_HOME}/social_network/static/*.csv ${LDBC_SNB_DATAGEN_HOME}/social_network/

# delete any existing data from datagen
rm -Rf ${LDBC_SNB_DATAGEN_HOME}/social_network ${LDBC_SNB_DATAGEN_HOME}/substitution_parameters

# copy data from validation to datagen
cp -a ../validation/SF01/social_network ${LDBC_SNB_DATAGEN_HOME}/social_network
cp -a ../validation/SF01/substitution_parameters ${LDBC_SNB_DATAGEN_HOME}/substitution_parameters/
