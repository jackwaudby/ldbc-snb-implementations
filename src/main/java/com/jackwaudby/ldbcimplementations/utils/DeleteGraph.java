package com.jackwaudby.ldbcimplementations.utils;

import org.apache.log4j.Logger;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;

public class DeleteGraph {

    private static Logger LOGGER = Logger.getLogger(DeleteGraph.class);

    public static void main( String[] args ) {

        JanusGraph graph = JanusGraphFactory.open("/Users/jackwaudby/janusgraph-0.4.0-hadoop2/conf/janusgraph-berkeleyje.properties");
        GraphTraversalSource g = graph.traversal(); // create traversal source

        GraphStats.elementCount(g);                 // get stats
        LOGGER.info("Dropping Graph");
        g.V().drop().iterate();                     // drop graph
        graph.tx().commit();                        // commit changes
        LOGGER.info("Graph Dropped");
        GraphStats.elementCount(g);                 // get stats
        CloseGraph.closeGraph(g);                   // close graph
        System.exit(0);                      // close program
    }



}


