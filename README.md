# LDBC SNB Implementation for JanusGraph

## JanusGraph Overview ##

JanusGraph is a graph layer that is reliant on pluggable, external systems to provide persistent storage and indexing; Apache Cassandra, Apache HBase and Oracle Berkeley DB JE are supported as storage backends. JanusGraph itself is a set a Java classes that need to be invoked by a calling process, it is distributed with adapted versions of the Gremlin Console and Gremlin Server which can play this role. 

The purpose of this implementation was to provide initial insight into the cost of _serializability_ in graph databases. JanusGraph inherits the consistency semantics of the chosen storage backend. Cassandra and HBase do not provide ACID transactions, hence the storage backend used was BerkeleyDB; the caveat is BerkeleyDB is non-distributed, i.e. it does not support horizontal partitioning. 

JanusGraph can be hosted in TinkerPop's Gremlin Server, exposing the graph as endpoint from which clients can connect. Gremlin Server allows clients to connect via a WebSockets connection and/or a HTTP connection. The advised approach is Websockets as results are placed directly into variables of the appropriate type for a given language. The downside of this approach is a reduced control over transactions, it is not possible to combined multiple traversals into a single transaction. Connection via HTTP allows for submission of Gremlin-based scripts, which provides full control over transactions - this is the approach adopted in this implementation. 
## JanusGraph Transactions ##

Every graph operation in JanusGraph occurs within the context of a transaction. Transactions in JanusGraph do not have to be explicitly opened, but **must** be explicitly committed; each thread opens its own transaction with the first operation on the graph. 

JanusGraph differentiates between potentially temporary and permanent failures. Potentially temporary failures are those related to resource unavailability and IO hiccups. JanusGraph automatically tries to recover from temporary failures by retrying to persist the transactional state after some delay. The number of retry attempts and the retry delay are configurable; default maximum wait-time for reads/writes is 100000_ms_ within this window JanusGraph will exponentially backoff and retry the transaction. Permanent failures can be caused by complete connection loss, hardware failure, a concurrent transaction has acquired a conflicting lock or a concurrent transaction has modified a value that has been read. Depending on the transaction semantics one can recover from a lock contention failure by rerunning the entire transaction. 

A good strategy is to rollback a transaction at the start of a request and commit or roll back at the end of the request, to prevent a transactional leak between requests. 

Reference: 
+ [JanusGraph Documentation](https://docs.janusgraph.org/basics/transactions/)
+ [TinkerPop Documentation](http://tinkerpop.apache.org/docs/current/reference/#transactions)

## Implementation Validation ##

Two validation sets were used during implementation. Validation set 1 was scrapped from the [`ldbc_interactive_validation`](https://github.com/ldbc/ldbc_snb_interactive_validation) repository; this validation set is now deprecated. Validation set 2 was generated using the [Cypher implementation repository](https://github.com/ldbc/ldbc_snb_implementations).

## Step-by-step Guide ##

Assuming JanusGraph is installed:
1. Delete existing database using `delete-db.sh`
2. Load chosen validation set in DATAGEN home directory (this is where the loader looks for the data) using `choose-validation.sh <1 or 2>`
3. Validation set 1 requires preprocessing using `preprocessing.sh`
4. Load schema, indexes, vertices and edges using `complete-loader.sh`
5. Start JanusGraph Server: `start-gremlin-server.sh`
6. Set driver configuration for validation in `interactive-validate.properties`
7. Run validation:`run-validation.sh`


N.B. Starting JanusGraph console (used for testing): 
```
bin/gremlin.sh
graph = JanusGraphFactory.open('conf/janusgraph-berkeleyje.properties')
g = graph.traversal()
```

## Validation Sets ##

### Validation Set 1 ###

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
+ Complex Reads 11/14
+ Updates 8/8

Missing handler implementations for 1 operation type(s)
+ LdbcQuery14

| Operation  | Incorrect Result |
|------------|------------------|
| LdbcQuery3 | 1                |
| LdbcQuery10| 2                |
| LdbcQuery12| 5                |

Issue: 
+ Expected result seems to be ordered descending by `totalCount` and then `personId` (ascending). My implementation of `LdbcQuery3` orders by `countX` (descending) then `personId` (ascending) as per the [specification](https://ldbc.github.io/ldbc_snb_docs/ldbc-snb-specification.pdf).
+ My `LdbcQuery12` is not ordering by `personId`.
+ My `LdbcQuery10` is producing results that do not exist in the expected answer.

### Validation Set 2 ###


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

Missing handler implementations for 0 operation type(s)


| Operation    | Incorrect Result |
|--------------|------------------|
| `LdbcQuery1` | 1                |
| `LdbcQuery14`| 1                |


Issues: 
+ Validation's expected answer for `LdbcQuery1` is including the start person which it should not (see Complex Read 1 in [specification](https://ldbc.github.io/ldbc_snb_docs/ldbc-snb-specification.pdf)). This could imply a problem with the Cypher query used to generate the validation set.
+ Implementation of `LdbcQuery14` does not handle the case when there are multiple shortest paths. 










