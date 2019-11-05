package com.jackwaudby.ldbcimplementations;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.apache.log4j.Logger;

import static com.jackwaudby.ldbcimplementations.utils.CloseGraph.closeGraph;
import static com.jackwaudby.ldbcimplementations.utils.LoadSchemaIndexes.loadSchemaIndexes;

/**
 * This script creates embedded connection with JanusGraph and loads schema and indexes.
 */
public class SchemaIndexesLoader {

    public static final Logger LOGGER = Logger.getLogger(SchemaIndexesLoader.class);

    public static void main(String[] args) {

        String janusGraphHome = System.getenv("JANUSGRAPH_HOME");             // get JanusGraph home directory
        JanusGraph graph = JanusGraphFactory.open(janusGraphHome
                + "/conf/janusgraph-berkeleyje.properties");
        loadSchemaIndexes(graph);                                                   // load schema
        GraphTraversalSource g = graph.traversal();                                 // create traversal source
        JanusGraphManagement schema = graph.openManagement();                       // get schema
        LOGGER.info("Schema and Indexes: \n" + schema.printSchema());
        schema.commit();                                                            // open connection to JanusGraph
        closeGraph(g);                                                              // close graph traversal source
        LOGGER.info("Closing Graph Traversal Source");
        graph.close();                                                              // close graph
        LOGGER.info("Closing JanusGraph connection");

    }
}
