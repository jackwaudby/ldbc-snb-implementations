# LDBC SNB Implementation for JanusGraph

This implementation assumes JanusGraph is backed by BerkeleyDB.

```
# set this to the janusgraph repo
export JANUSGRAPH_HOME=`pwd`
# set this to the DATAGEN repo
export LDBC_SNB_DATAGEN_HOME=`pwd`
```

## Step-by-step Guide ##

Navigate to the `scripts` directory.
```
# delete berkeleydb database
./delete-db.sh
# load validation dataset into DATAGEN (this is where the loader looks for the data)
./load-validation.sh
# load schema, indexes, vertices and edges
./complete-loader.sh
# start janusgraph server
./start-gremlin-server.sh
# set driver configuration for validation in interactive-validate.properties
# run validation
./run-validation.sh
```

## JanusGraph Console ##

Used for testing:
```
bin/gremlin.sh
graph = JanusGraphFactory.open('conf/janusgraph-berkeleyje-test.properties')
graph = JanusGraphFactory.open('conf/janusgraph-berkeleyje.properties')

g = graph.traversal()
```

## Validation Set ##

|  Validation Set   |              |
|-------------------|--------------|
| Data Format:      | CSVComposite |
| Operations:       | 1321         |
| SF:               | 0.3          |
| Vertex Count:     | ~210k        |
| Edge Count:       | ~1.09m       |
| Load Time:        | ~2:30mins    |
| Index Time:       | ~2:45mins    |

Passing Validation:
+ Short Reads 7/7
+ Complex Reads 12/14
+ Updates 8/8

Missing handler implementations for 0 operation type(s)


| Operation    | Incorrect Result |
|--------------|------------------|
| `LdbcQuery1` | 1                |
| `LdbcQuery14`| 1                |


Issues:
+ Validation's expected answer for `LdbcQuery1` is including the start person which it should not (see Complex Read 1 in [specification](https://ldbc.github.io/ldbc_snb_docs/ldbc-snb-specification.pdf)). This could imply a problem with the Cypher query used to generate the validation set.
+ My `LdbcQuery12` is not ordering by `personId`.
+ Implementation of `LdbcQuery14` does not handle the case when there are multiple shortest paths.
+ Problem with `LdbcShortQuery2PersonPosts` and `LdbcShortQuery7MessageReplies` not returning results.
