package com.jackwaudby.ldbcimplementations.utils;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

/**
 * This script provides a method for closing a Graph Traversal Source.
 */
public class CloseGraph {

    public static void closeGraph(GraphTraversalSource g) {
        try {
            g.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
