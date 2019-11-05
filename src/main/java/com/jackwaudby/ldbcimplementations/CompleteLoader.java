package com.jackwaudby.ldbcimplementations;

import org.apache.log4j.Logger;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.schema.JanusGraphManagement;

import static com.jackwaudby.ldbcimplementations.utils.BulkLoadEdges.bulkLoadEdges;
import static com.jackwaudby.ldbcimplementations.utils.BulkLoadVertices.bulkLoadVertices;
import static com.jackwaudby.ldbcimplementations.utils.CloseGraph.closeGraph;
import static com.jackwaudby.ldbcimplementations.utils.LoadSchemaIndexes.loadSchemaIndexes;

/**
 * This script creates embedded connection with JanusGraph and loads schema, indexes and data.
 */
public class CompleteLoader {

    public static final Logger LOGGER = Logger.getLogger(CompleteLoader.class);


    public static void main(String[] args) {

        String janusGraphHome = System.getenv("JANUSGRAPH_HOME");             // get JanusGraph home directory
        String ldbcSnbDatagenHome = System.getenv("LDBC_SNB_DATAGEN_HOME");   // path to generated data directory
        LOGGER.info("Loading Configuration:");
        LOGGER.info("$JANUSGRAPH_HOME: " + janusGraphHome);
        LOGGER.info("$LDBC_SNB_DATAGEN_HOME: " + ldbcSnbDatagenHome);


        JanusGraph graph = JanusGraphFactory.open(janusGraphHome
                + "/conf/janusgraph-berkeleyje-bulk.properties");                        // open connection to JanusGraph

        GraphTraversalSource g = graph.traversal();                                 // create traversal source
        loadSchemaIndexes(graph);                                                   // load schema
        JanusGraphManagement schema = graph.openManagement();                       // get schema
        LOGGER.info("Schema and Indexes: \n" + schema.printSchema());
        schema.commit();
        String pathToData = ldbcSnbDatagenHome + "/social_network/";                // path to data
        LOGGER.info("Loading Vertices");
        bulkLoadVertices(pathToData,graph,g);                                       // load vertices
        LOGGER.info("Loading Edges");
        bulkLoadEdges(pathToData,graph,g);                                          // load edges
        closeGraph(g);                                                              // close graph traversal source
        LOGGER.info("Closing Graph Traversal Source");
        graph.close();                                                              // close graph
        LOGGER.info("Closing JanusGraph connection");

    }
}
