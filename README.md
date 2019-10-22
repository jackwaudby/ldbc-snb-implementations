# LDBC SNB Implementation for JanusGraph

Start JanusGraph Server:
`bin/gremlin-server.sh conf/gremlin-server/gremlin-server-berkeleyje.yaml`

Run validation:
`java -cp "target/janusgraph-1.0-SNAPSHOT.jar:target/dependencies/*" com.ldbc.driver.Client -P validation/interactive-validate.properties`

Start JanusGraph console (used for testing): 
```
bin/gremlin.sh
graph = JanusGraphFactory.open('conf/janusgraph-berkeleyje.properties')
g = graph.traversal()
```

Passing Validation:
+ Short Reads 7/7
+ Complex Reads 8/14
+ Updates 8/8

| Update | GraphElement                     | BulkLoad | Validation | Total |   
|--------|----------------------------------|----------|------------|-------|
| 1      | `(Person)`                       | 2747     | 22         | 2769  |
| 2      | `(Person)-[:likes]->(Post)`      | 36658    | 283        | 36941 |
| 3      | `(Person)-[:likes]->(Comment)`   | 46737    | 162        | 46899 |
| 4      | `(Forum)`                        | 10032    | 22         | 10054 |
| 5      | `(Person)-[:hasMember]->(Forum)` | 145407   | 1056       | 146463|
| 6      | `(Post)`                         | 79339    | 205        | 79544 |  
| 7      | `(Comment)`                      | 94241    | 283        | 94524 |
| 8      | `(Person)-[:knows]->(Person)`    | 31046    | 119        | 31165 |   




