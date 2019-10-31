#!/usr/bin/env bash

echo "loading validation data set_0$1"

# delete data from datagen
rm -Rf ~/Documents/ldbc/ldbc_snb_datagen/social_network ~/Documents/ldbc/ldbc_snb_datagen/substitution_parameters

# copy data from validation to datagen
cp -a ~/Documents/janusgraph/validation/set_0$1/social_network ~/Documents/ldbc/ldbc_snb_datagen/social_network
cp -a ~/Documents/janusgraph/validation/set_0$1/substitution_parameters ~/Documents/ldbc/ldbc_snb_datagen/substitution_parameters
