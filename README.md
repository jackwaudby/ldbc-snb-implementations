# LDBC SNB Implementation for JanusGraph

## JanusGraph Overview ##

JanusGraph is a graph layer that is reliant on pluggable, external systems to provide persistent storage and indexing; 
Apache Cassandra, Apache HBase and Oracle Berkeley DB JE are supported as storage backends. 
JanusGraph itself is a set a Java classes that need to be invoked by a calling process, it is distributed with adapted 
versions of the Gremlin Console and Gremlin Server which can play this role. 

The purpose of this implementation was to provide initial insight into the cost of _serializability_ in graph databases.
 JanusGraph inherits the consistency semantics of the chosen storage backend. 
 Cassandra and HBase do not provide ACID transactions, hence the storage backend used was BerkeleyDB; 
 the caveat is BerkeleyDB is non-distributed, i.e. it does not support horizontal partitioning. 

JanusGraph can be hosted in TinkerPop's Gremlin Server, exposing the graph as endpoint from which clients can connect. 
Gremlin Server allows clients to connect via a WebSockets connection and/or a HTTP connection. 
The advised approach is Websockets as results are placed directly into variables of the appropriate type for a given 
language. 
The downside of this approach is a reduced control over transactions, it is not possible to combined multiple traversals 
into a single transaction. 
Connection via HTTP allows for submission of Gremlin-based scripts, which provides full control over transactions - 
this is the approach adopted in this implementation. 

## JanusGraph Transactions ##

Every graph operation in JanusGraph occurs within the context of a transaction. 
Transactions in JanusGraph do not have to be explicitly opened, but **must** be explicitly committed; each thread opens 
its own transaction with the first operation on the graph. 

JanusGraph differentiates between potentially temporary and permanent failures. 
Potentially temporary failures are those related to resource unavailability and IO hiccups. 
JanusGraph automatically tries to recover from temporary failures by retrying to persist the transactional state after 
some delay. The number of retry attempts and the retry delay are configurable; default maximum wait-time for 
reads/writes is 100000_ms_ within this window JanusGraph will exponentially backoff and retry the transaction. 
Permanent failures can be caused by complete connection loss, hardware failure, a concurrent transaction has 
acquired a conflicting lock or a concurrent transaction has modified a value that has been read. Depending on the 
transaction semantics one can recover from a lock contention failure by rerunning the entire transaction. 

A good strategy is to rollback a transaction at the start of a request and commit or roll back at the end of the request, 
to prevent a transactional leak between requests. 

References: 
+ [JanusGraph Documentation](https://docs.janusgraph.org/basics/transactions/)
+ [TinkerPop Documentation](http://tinkerpop.apache.org/docs/current/reference/#transactions)

## Implementation Validation ##

The validation se were used during the implementation was generated using the 
[Cypher implementation repository](https://github.com/ldbc/ldbc_snb_implementations).


## Step-by-step Guide ##

Assuming JanusGraph is installed:
1. Delete existing BerkeleyDB database using `delete-db.sh`
2. Load chosen validation set into LDBC DATAGEN home directory (this is where the loader looks for the data) using `load-validation.sh`
3. Load schema, indexes, vertices and edges using `complete-loader.sh`
4. Start JanusGraph Server: `start-gremlin-server.sh`
5. Set driver configuration for validation in `interactive-validate.properties`
6. Run validation:`run-validation.sh`


N.B. Starting JanusGraph console (used for testing): 
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








