package com.jackwaudby.ldbcimplementations;

import org.apache.log4j.Logger;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.janusgraph.core.*;

import static com.jackwaudby.ldbcimplementations.utils.BulkLoadEdges.bulkLoadEdges;
import static com.jackwaudby.ldbcimplementations.utils.CloseGraph.closeGraph;

/**
 * This script creates embedded connection with JanusGraph and loads edges.
 */
public class EdgeLoader {

    public static final Logger LOGGER = Logger.getLogger(EdgeLoader.class);

    public static void main(String[] args) {

        String janusGraphHome = System.getenv("JANUSGRAPH_HOME");             // get JanusGraph home directory
        String ldbcSnbDatagenHome = System.getenv("LDBC_SNB_DATAGEN_HOME");   // path to generated data directory
        LOGGER.info("$JANUSGRAPH_HOME: " + janusGraphHome);
        LOGGER.info("$LDBC_SNB_DATAGEN_HOME: " + ldbcSnbDatagenHome);
        JanusGraph graph = JanusGraphFactory.open(janusGraphHome
                + "/conf/janusgraph-berkeleyje-bulk.properties");                        // open connection to JanusGraph
        GraphTraversalSource g = graph.traversal();                                 // create traversal source
        String pathToData = ldbcSnbDatagenHome + "/social_network/";                // path to data
        bulkLoadEdges(pathToData,graph,g);                                          // load edges
        closeGraph(g);                                                              // close graph traversal source
        LOGGER.info("Closing Graph Traversal Source");
        graph.close();                                                              // close graph
        LOGGER.info("Closing JanusGraph connection");

    }

}
