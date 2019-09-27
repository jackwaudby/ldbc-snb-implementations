#!/usr/bin/env bash

# This script merges person_email_emailaddress_0_0.csv and person_speaks_language_0_0.csv in person_0.0.csv
# All properties for Person vertices will exist in the same file
# This makes loading the data into JanusGraph easier


#PATH_TO_DATA="/Users/jackwaudby/Documents/ldbc/ldbc_snb_datagen/test_data/social_network"
PATH_TO_DATA="/Users/jackwaudby/Documents/ldbc/ldbc_snb_interactive_validation/neo4j/neo4j--validation_set/social_network/string_date"

merge_file () {

if [ "$1" = "${PATH_TO_DATA}/person_email_emailaddress_0_0.csv" ]; then
    header='1s;^;id|email\n;'
    echo "Merging email addresses"
elif [ "$1" = "${PATH_TO_DATA}/person_speaks_language_0_0.csv" ]; then
    header='1s;^;id|language\n;'
    echo "Merging languages"
else
    echo "Error!"
    exit 1
fi

# group by id
sed 1d "$1" | awk -F"|" '{if(a[$1])a[$1]=a[$1]":"$2; else a[$1]=$2;}END{for (i in a) print i "|" a[i];}' | sort -n > ${PATH_TO_DATA}/temp.csv
# add header
sed -i ${header} ${PATH_TO_DATA}/temp.csv
# join with person_0_0.csv on id
awk -F"|" 'NR==FNR {h[$1]=$2;next} {print $0 "|" h[$1]}' ${PATH_TO_DATA}/temp.csv ${PATH_TO_DATA}/person_0_0.csv > ${PATH_TO_DATA}/person_0_0_temp.csv

mv ${PATH_TO_DATA}/person_0_0_temp.csv ${PATH_TO_DATA}/person_0_0.csv
rm ${PATH_TO_DATA}/temp.csv


}

merge_file ${PATH_TO_DATA}/person_email_emailaddress_0_0.csv
merge_file ${PATH_TO_DATA}/person_speaks_language_0_0.csv

rm ${PATH_TO_DATA}/person_email_emailaddress_0_0.csv
rm ${PATH_TO_DATA}/person_speaks_language_0_0.csv

# TODO: Split into vertices and edges folders


