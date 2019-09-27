package com.jackwaudby.ldbcimplementations.utils;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

public class CloseGraph {

    public static void closeGraph(GraphTraversalSource g) {

        try {
            g.close();
        } catch (Exception e) {
            System.out.println(e);
        }
    }

}
