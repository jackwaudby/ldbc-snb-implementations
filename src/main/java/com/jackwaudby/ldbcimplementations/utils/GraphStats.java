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
        Long person = g.V().hasLabel("Person").count().next();
        Long forum = g.V().hasLabel("Forum").count().next();
        Long post = g.V().hasLabel("Post").count().next();
        Long comment = g.V().hasLabel("Comment").count().next();
        LOGGER.info("Person Count: "+ person);
        LOGGER.info("Forum Count: "+ forum);
        LOGGER.info("Post Count: "+ post);
        LOGGER.info("Comment Count: "+ comment);

//        Long ec = g.E().count().next();
//        LOGGER.info("Edge Count: "+ ec);

    }

}
