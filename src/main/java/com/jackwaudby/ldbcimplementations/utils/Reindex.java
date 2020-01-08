package com.jackwaudby.ldbcimplementations.utils;

import com.jackwaudby.ldbcimplementations.CompleteLoader;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.SchemaViolationException;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.SchemaAction;

import java.util.concurrent.ExecutionException;

public class Reindex {

    /**
     * Loads indexes
     * @param graph JanusGraph instance
     */
    public static void reindex (JanusGraph graph) {

        JanusGraphManagement mgmt = graph.openManagement(); // create management object

        try {
            // define graph indexes
            mgmt.updateIndex(mgmt.getGraphIndex("byPlaceId"), SchemaAction.REINDEX).get();
            mgmt.updateIndex(mgmt.getGraphIndex("byCommentId"), SchemaAction.REINDEX).get();
            mgmt.updateIndex(mgmt.getGraphIndex("byOrganisationId"), SchemaAction.REINDEX).get();
            mgmt.updateIndex(mgmt.getGraphIndex("byForumId"), SchemaAction.REINDEX).get();
            mgmt.updateIndex(mgmt.getGraphIndex("byPersonId"), SchemaAction.REINDEX).get();
            mgmt.updateIndex(mgmt.getGraphIndex("byPostId"), SchemaAction.REINDEX).get();
            mgmt.updateIndex(mgmt.getGraphIndex("byTagId"), SchemaAction.REINDEX).get();
            mgmt.updateIndex(mgmt.getGraphIndex("byTagClassId"), SchemaAction.REINDEX).get();

            // commit schema
            mgmt.commit();

        } catch (
                SchemaViolationException e) {
            CompleteLoader.LOGGER.error("Indexes may already be defined: " + e);
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }
}
