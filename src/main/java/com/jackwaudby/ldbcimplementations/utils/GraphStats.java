package com.jackwaudby.ldbcimplementations.utils;

import org.apache.log4j.Logger;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

public class GraphStats {

    static final Logger LOGGER = Logger.getLogger(GraphStats.class);

    public static void elementCount(GraphTraversalSource g){

        Long vc = g.V().count().next();
        LOGGER.info("Vertex Count: "+ vc);
        Long ec = g.E().count().next();
        LOGGER.info("Edge Count: "+ ec);

    }

}
