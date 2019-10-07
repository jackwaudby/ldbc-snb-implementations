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

        Long person = g.V().hasLabel("Person").count().next();
        Long forum = g.V().hasLabel("Forum").count().next();
        Long post = g.V().hasLabel("Post").count().next();
        Long comment = g.V().hasLabel("Comment").count().next();
        Long likesPost = g.V().hasLabel("Person").outE().hasLabel("likes").inV().hasLabel("Post").count().next();
        Long hasMember = g.E().hasLabel("hasMember").count().next();
        Long likesComment = g.V().hasLabel("Person").outE().hasLabel("likes").inV().hasLabel("Comment").count().next();
        Long knows = g.E().hasLabel("knows").count().next();

        LOGGER.info("(Person): "+ person);
        LOGGER.info("(Person)-[:likes]->(Post): "+ likesPost);
        LOGGER.info("(Person)-[:likes]->(Comment): "+ likesComment);
        LOGGER.info("(Forum): "+ forum);
        LOGGER.info("(Person)-[:hasMember]->(Forum): "+ hasMember);
        LOGGER.info("(Post): "+ post);
        LOGGER.info("(Comment): "+ comment);
        LOGGER.info("(Person)-[:knows]->(Person): "+ knows);


    }

}
