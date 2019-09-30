package com.jackwaudby.ldbcimplementations;

import org.apache.log4j.Logger;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.janusgraph.core.*;
import org.janusgraph.core.schema.JanusGraphManagement;

import static com.jackwaudby.ldbcimplementations.utils.BulkLoadEdges.bulkLoadEdges;
import static com.jackwaudby.ldbcimplementations.utils.BulkLoadVertices.bulkLoadVertices;
import static com.jackwaudby.ldbcimplementations.utils.CloseGraph.closeGraph;
import static com.jackwaudby.ldbcimplementations.utils.GraphStats.elementCount;
import static com.jackwaudby.ldbcimplementations.utils.LoadSchemaIndexes.loadSchemaIndexes;

/**
 * This script creates embedded connection with JanusGraph and loads schema, indexes and data.
 */
public class GraphLoader {

    public static final Logger LOGGER = Logger.getLogger(GraphLoader.class);

    public static void main(String[] args) {

        String janusGraphHome = System.getenv("JANUSGRAPH_HOME");             // get JanusGraph home directory
        String ldbcSnbDatagenHome = System.getenv("LDBC_SNB_DATAGEN_HOME");   // path to generated data directory
        LOGGER.info("$JANUSGRAPH_HOME: " + janusGraphHome);
        LOGGER.info("$LDBC_SNB_DATAGEN_HOME: " + ldbcSnbDatagenHome);
        JanusGraph graph = JanusGraphFactory.open(janusGraphHome
                + "/conf/janusgraph-berkeleyje.properties");                        // open connection to JanusGraph
        loadSchemaIndexes(graph); // load schema
        GraphTraversalSource g = graph.traversal();                                 // create traversal source
        JanusGraphManagement schema = graph.openManagement();                       // get schema
        LOGGER.info("Schema and Indexes: \n" + schema.printSchema());
        schema.commit();                                                            // commit schema
        String pathToData = ldbcSnbDatagenHome + "/social_network/";                // path to data
        bulkLoadVertices(pathToData,graph,g);                                       // load vertices
        bulkLoadEdges(pathToData,graph,g);                                          // load edges
        elementCount(g);                                                            // print graph stats
        closeGraph(g);                                                              // close graph traversal source
        LOGGER.info("Closing Graph Traversal Source");
        graph.close();                                                              // close graph
        LOGGER.info("Closing JanusGraph connection");

    }

}
