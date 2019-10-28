# LDBC SNB Implementation for JanusGraph

## Step-by-step Guide ##

1. Delete existing database using `delete-db.sh`
2. Load chosen validation set in DATAGEN home directory (this is where the loader looks for the data) using `choose-validation.sh`
3. If data format is CSV then preprocess using `preprocessing.sh`
3. Load schema, indexes, vertices and edges 
5. Start JanusGraph Server: `bin/gremlin-server.sh conf/gremlin-server/gremlin-server-berkeleyje.yaml`
6. Set driver configuration for validation in `interactive-validate.properties`
7. Run validation:`java -cp "target/janusgraph-1.0-SNAPSHOT.jar:target/dependencies/*" com.ldbc.driver.Client -P validation/interactive-validate.properties`


N.B. Starting JanusGraph console (used for testing): 
```
bin/gremlin.sh
graph = JanusGraphFactory.open('conf/janusgraph-berkeleyje.properties')
g = graph.traversal()
```

## Validation Sets ##

This repository contains 2 validation sets. Each sets contains a `social_network` and `substitution_parameters`, along with `validation_params.csv`. 

### Validation Set 1 ###

This validation set was taken from the Neo4j directory in the [`ldbc_interactive_validation`](https://github.com/ldbc/ldbc_snb_interactive_validation) repo

|   Validation Set 1 |        |
|--------------------|--------|
| Data Format:       | CSV    |
| Operations:        | 11929  |
| SF:                | 0.3    |
| Vertex Count:      | ~212k  |
| Edge Count:        | ~1.11m |
| Vertex Load Time:  | 75s    |
| Edge Load Time:    | 496s   |

Passing Validation:
+ Short Reads 7/7
+ Complex Reads 9/14
+ Updates 8/8

Missing handler implementations for 5 operation types
+ LdbcQuery10
+ LdbcQuery12
+ LdbcQuery13
+ LdbcQuery14
+ LdbcQuery5

| Operation  | Incorrect Result |
|------------|------------------|
| LdbcQuery3 | 1                |

Issue: expected result seems to be ordered descending by `totalCount` and then `personId` (ascending). My implementation of `LdbcQuery3` orders by `countX` (descending) then `personId` (ascending) as per the [specification](https://ldbc.github.io/ldbc_snb_docs/ldbc-snb-specification.pdf).

### Validation Set 2 ###

This validation set was generated using the [Cypher implementation repository](https://github.com/ldbc/ldbc_snb_implementations). 

|  Validation Set 2 |              |
|-------------------|--------------|
| Data Format:      | CSVComposite |
| Operations:       | 1321         |
| SF:               | 0.3          |
| Vertex Count:     | ~210k        |
| Edge Count:       | ~1.09m       |
| Vertex Load Time: | 74s          |
| Edge Load Time:   | 793s         |


Passing Validation:
+ Short Reads 7/7
+ Complex Reads 12/14
+ Updates 8/8

Missing handler implementations for 2 operation types
+ LdbcQuery10
+ LdbcQuery14

| Operation  | Incorrect Result |
|------------|------------------|
| `LdbcQuery1` | 1              |



Issues: 
+ Validation's expected answer for `LdbcQuery1` is including the start person which it should not (see Complex Read 1 in [specification](https://ldbc.github.io/ldbc_snb_docs/ldbc-snb-specification.pdf)). This could imply a problem with the Cypher query used to generate the validation set.










