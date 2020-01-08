package com.jackwaudby.ldbcimplementations.utils;

import com.jackwaudby.ldbcimplementations.VertexLoader;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.SchemaViolationException;
import org.janusgraph.core.schema.JanusGraphManagement;

public class LoadIndexes {

    /**
     * Loads indexes
     * @param graph JanusGraph instance
     */
    public static void loadIndexes (JanusGraph graph) {

        JanusGraphManagement mgmt = graph.openManagement(); // create management object

        try {
            // define graph indexes
            mgmt.buildIndex("byPlaceId", Vertex.class).addKey(mgmt.getPropertyKey("id")).indexOnly(mgmt.getVertexLabel("Place")).unique().buildCompositeIndex();
            mgmt.buildIndex("byCommentId", Vertex.class).addKey(mgmt.getPropertyKey("id")).indexOnly(mgmt.getVertexLabel("Comment")).unique().buildCompositeIndex();
            mgmt.buildIndex("byOrganisationId", Vertex.class).addKey(mgmt.getPropertyKey("id")).indexOnly(mgmt.getVertexLabel("Organisation")).unique().buildCompositeIndex();
            mgmt.buildIndex("byForumId", Vertex.class).addKey(mgmt.getPropertyKey("id")).indexOnly(mgmt.getVertexLabel("Forum")).unique().buildCompositeIndex();
            mgmt.buildIndex("byPersonId", Vertex.class).addKey(mgmt.getPropertyKey("id")).indexOnly(mgmt.getVertexLabel("Person")).unique().buildCompositeIndex();
            mgmt.buildIndex("byPostId", Vertex.class).addKey(mgmt.getPropertyKey("id")).indexOnly(mgmt.getVertexLabel("Post")).unique().buildCompositeIndex();
            mgmt.buildIndex("byTagId", Vertex.class).addKey(mgmt.getPropertyKey("id")).indexOnly(mgmt.getVertexLabel("Tag")).unique().buildCompositeIndex();
            mgmt.buildIndex("byTagClassId", Vertex.class).addKey(mgmt.getPropertyKey("id")).indexOnly(mgmt.getVertexLabel("TagClass")).unique().buildCompositeIndex();

            // commit schema
            mgmt.commit();

        } catch (
                SchemaViolationException e) {
            VertexLoader.LOGGER.error("Indexes may already be defined: " + e);
        }
    }
}
