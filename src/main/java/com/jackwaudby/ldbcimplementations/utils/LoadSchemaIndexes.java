package com.jackwaudby.ldbcimplementations.utils;

import com.jackwaudby.ldbcimplementations.GraphLoader;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.Multiplicity;
import org.janusgraph.core.SchemaViolationException;
import org.janusgraph.core.schema.JanusGraphManagement;

import java.util.Date;

public class LoadSchemaIndexes {

    /**
     * Loads schema and indexes
     * @param graph JanusGraph instance
     */
    public static void loadSchemaIndexes (JanusGraph graph) {

        JanusGraphManagement mgmt = graph.openManagement(); // create management object

        try {
            // define vertex labels
            mgmt.makeVertexLabel("Place").make();
            mgmt.makeVertexLabel("Comment").make();
            mgmt.makeVertexLabel("Forum").make();
            mgmt.makeVertexLabel("Person").make();
            mgmt.makeVertexLabel("Post").make();
            mgmt.makeVertexLabel("Tag").make();
            mgmt.makeVertexLabel("TagClass").make();
            mgmt.makeVertexLabel("Organisation").make();


            // define edge labels and USAGE
            mgmt.makeEdgeLabel("containerOf").multiplicity(Multiplicity.SIMPLE).make();
            mgmt.makeEdgeLabel("hasCreator").multiplicity(Multiplicity.SIMPLE).make();
            mgmt.makeEdgeLabel("hasInterest").multiplicity(Multiplicity.SIMPLE).make();
            mgmt.makeEdgeLabel("hasMember").multiplicity(Multiplicity.SIMPLE).make();
            mgmt.makeEdgeLabel("hasModerator").multiplicity(Multiplicity.SIMPLE).make();
            mgmt.makeEdgeLabel("hasTag").multiplicity(Multiplicity.SIMPLE).make();
            mgmt.makeEdgeLabel("isLocatedIn").multiplicity(Multiplicity.SIMPLE).make();
            mgmt.makeEdgeLabel("isPartOf").multiplicity(Multiplicity.SIMPLE).make();
            mgmt.makeEdgeLabel("isSubclassOf").multiplicity(Multiplicity.SIMPLE).make();
            mgmt.makeEdgeLabel("likes").multiplicity(Multiplicity.SIMPLE).make();
            mgmt.makeEdgeLabel("knows").multiplicity(Multiplicity.SIMPLE).make();
            mgmt.makeEdgeLabel("replyOf").multiplicity(Multiplicity.SIMPLE).make();
            mgmt.makeEdgeLabel("studyAt").multiplicity(Multiplicity.SIMPLE).make();
            mgmt.makeEdgeLabel("workAt").multiplicity(Multiplicity.SIMPLE).make();
            mgmt.makeEdgeLabel("hasType").multiplicity(Multiplicity.SIMPLE).make();

            // define vertex property keys
            mgmt.makePropertyKey("id").dataType(Long.class).cardinality(Cardinality.SINGLE).make();         // Forum, Post/Comment (Message), Company/University (Organisation), Person, City/Country/Continent (Place), Tag, TagClass
            mgmt.makePropertyKey("title").dataType(String.class).cardinality(Cardinality.SINGLE).make();        // Forum
            mgmt.makePropertyKey("creationDate").dataType(Date.class).cardinality(Cardinality.SINGLE).make();   // Forum, Post/Comment (Message), Person, :knows, :likes
            mgmt.makePropertyKey("browserUsed").dataType(String.class).cardinality(Cardinality.SINGLE).make();  // Post/Comment (Message), Person
            mgmt.makePropertyKey("locationIP").dataType(String.class).cardinality(Cardinality.SINGLE).make();   // Post/Comment (Message)
            mgmt.makePropertyKey("content").dataType(String.class).cardinality(Cardinality.SINGLE).make();      // Post/Comment (Message)
            mgmt.makePropertyKey("length").dataType(Integer.class).cardinality(Cardinality.SINGLE).make();      // Post/Comment (Message
            mgmt.makePropertyKey("name").dataType(String.class).cardinality(Cardinality.SINGLE).make();         // Company/University (Organisation), City/Country/Continent (Place), Tag, TagClass
            mgmt.makePropertyKey("url").dataType(String.class).cardinality(Cardinality.SINGLE).make();          // Company/University (Organisation), City/Country/Continent (Place), Tag, TagClass
            mgmt.makePropertyKey("firstName").dataType(String.class).cardinality(Cardinality.SINGLE).make();    // Person
            mgmt.makePropertyKey("lastName").dataType(String.class).cardinality(Cardinality.SINGLE).make();     // Person
            mgmt.makePropertyKey("gender").dataType(String.class).cardinality(Cardinality.SINGLE).make();       // Person
            mgmt.makePropertyKey("birthday").dataType(Date.class).cardinality(Cardinality.SINGLE).make();       // Person
            mgmt.makePropertyKey("email").dataType(String.class).cardinality(Cardinality.SET).make();           // Person
            mgmt.makePropertyKey("speaks").dataType(String.class).cardinality(Cardinality.SET).make();        // Person, Post (Message)
            mgmt.makePropertyKey("language").dataType(String.class).cardinality(Cardinality.SINGLE).make();        // Person, Post (Message)
            mgmt.makePropertyKey("imageFile").dataType(String.class).cardinality(Cardinality.SINGLE).make();    // Post (Message)
            mgmt.makePropertyKey("type").dataType(String.class).cardinality(Cardinality.SINGLE).make();         // Company/University (Organisation), City/Country/Continent (Place)

            // define edge property keys
            mgmt.makePropertyKey("joinDate").dataType(Date.class).cardinality(Cardinality.SINGLE).make();       // :hasMember
            mgmt.makePropertyKey("classYear").dataType(Integer.class).cardinality(Cardinality.SINGLE).make();   // :studyAt
            mgmt.makePropertyKey("workFrom").dataType(Integer.class).cardinality(Cardinality.SINGLE).make();    // :workAt


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
            GraphLoader.LOGGER.error("Schema and Indexes may already be defined: " + e);
        }
    }
}
