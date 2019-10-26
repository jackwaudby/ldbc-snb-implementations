package com.jackwaudby.ldbcimplementations.utils;

import org.apache.log4j.Logger;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;

/**
 * This script provides basic graph statistics.
 */
public class GraphStats {

    private static final Logger LOGGER = Logger.getLogger(GraphStats.class);

    public static void main(String[] args) {

        JanusGraph graph = JanusGraphFactory.open("/Users/jackwaudby/janusgraph-0.4.0-hadoop2/conf/janusgraph-berkeleyje.properties");
        GraphTraversalSource g = graph.traversal(); // create traversal source
        elementCount(g);
        CloseGraph.closeGraph(g);
        graph.close();

    }

    public static void elementCount(GraphTraversalSource g){


        Long vertices = g.V().count().next();
        Long edges = g.E().count().next();

        LOGGER.info("Total Vertices: "+ vertices);
        LOGGER.info("Total Edges: "+ edges);



    }

}
