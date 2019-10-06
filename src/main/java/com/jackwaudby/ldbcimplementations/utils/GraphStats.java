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


//        Long vc = g.V().count().next();
//        LOGGER.info("Vertex Count: "+ vc);
        Long persons = g.V().hasLabel("Person").count().next();
        LOGGER.info("Vertex Count: "+ persons);
//        Long ec = g.E().count().next();
//        LOGGER.info("Edge Count: "+ ec);

    }

}
