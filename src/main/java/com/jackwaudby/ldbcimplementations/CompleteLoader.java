package com.jackwaudby.ldbcimplementations;

import org.apache.log4j.Logger;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.schema.JanusGraphManagement;

import java.util.HashMap;

import static com.jackwaudby.ldbcimplementations.utils.BulkLoadEdges.bulkLoadEdges;
import static com.jackwaudby.ldbcimplementations.utils.BulkLoadVertices.bulkLoadVertices;
import static com.jackwaudby.ldbcimplementations.utils.CloseGraph.closeGraph;
import static com.jackwaudby.ldbcimplementations.utils.LoadIndexes.loadIndexes;
import static com.jackwaudby.ldbcimplementations.utils.LoadSchema.loadSchema;

/**
 * This script creates embedded connection with JanusGraph and loads schema, indexes and data.
 */
public class CompleteLoader {

    public static final Logger LOGGER = Logger.getLogger(CompleteLoader.class);

    public static void main(String[] args) {

        LOGGER.info("Loading Configuration:");
        String janusGraphHome = System.getenv("JANUSGRAPH_HOME");             // get JanusGraph home directory
        String ldbcSnbDatagenHome = System.getenv("LDBC_SNB_DATAGEN_HOME");   // path to generated data directory
        LOGGER.info("$JANUSGRAPH_HOME: " + janusGraphHome);
        LOGGER.info("$LDBC_SNB_DATAGEN_HOME: " + ldbcSnbDatagenHome);

        String pathToData = ldbcSnbDatagenHome + "/social_network/";                // path to data

        HashMap<String, Object> ldbcIdToJanusGraphId = new HashMap<>();             // id map

        LOGGER.info("Opening JanusGraph connection");
        JanusGraph graph = JanusGraphFactory.open(janusGraphHome
                + "/conf/janusgraph-berkeleyje.properties");                        // open connection to JanusGraph

        LOGGER.info("Creating Graph Traversal Source");
        GraphTraversalSource g = graph.traversal();                                 // create traversal source
        loadSchema(graph);

        LOGGER.info("Loading Vertices");
        bulkLoadVertices(pathToData,graph,g,ldbcIdToJanusGraphId);

        LOGGER.info("Loading Edges");
        bulkLoadEdges(pathToData,graph,g,ldbcIdToJanusGraphId);

        LOGGER.info("Loading Index");
        loadIndexes(graph);

        LOGGER.info("Closing Graph Traversal Source");
        closeGraph(g);

        JanusGraphManagement schema = graph.openManagement();
        LOGGER.info("Schema: \n" + schema.printSchema());
        schema.commit();

        LOGGER.info("Closing JanusGraph connection");
        graph.close();

    }
}
