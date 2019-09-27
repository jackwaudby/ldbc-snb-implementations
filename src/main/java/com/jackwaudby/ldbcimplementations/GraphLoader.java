package com.jackwaudby.ldbcimplementations;

import com.jackwaudby.ldbcimplementations.utils.CloseGraph;
import com.jackwaudby.ldbcimplementations.utils.GraphStats;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.log4j.Logger;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.Multiplicity;
import org.janusgraph.core.schema.JanusGraphManagement;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.Reader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class GraphLoader {

    private static final Logger LOGGER = Logger.getLogger(GraphLoader.class);

    public static void main(String[] args) {

        // get JanusGraph home directory
        String janusGraphHome = System.getenv("JANUSGRAPH_HOME");
        LOGGER.info("$JANUSGRAPH_HOME: " + janusGraphHome);

        // open connection to JanusGraph
        JanusGraph graph = JanusGraphFactory.open(janusGraphHome + "/conf/janusgraph-berkeleyje.properties");

        // create management object
        // TODO: move schema creation to .groovy script that can be loaded on JanusGraph server startup; must check if schema in place, else create
        JanusGraphManagement mgmt = graph.openManagement();

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
        // TODO: check edge USAGE
        mgmt.makeEdgeLabel("containerOf").multiplicity(Multiplicity.MULTI).make();
        mgmt.makeEdgeLabel("hasCreator").multiplicity(Multiplicity.MULTI).make();
        mgmt.makeEdgeLabel("hasInterest").multiplicity(Multiplicity.MULTI).make();
        mgmt.makeEdgeLabel("hasMember").multiplicity(Multiplicity.MULTI).make();
        mgmt.makeEdgeLabel("hasModerator").multiplicity(Multiplicity.MULTI).make();
        mgmt.makeEdgeLabel("hasTag").multiplicity(Multiplicity.MULTI).make();
        mgmt.makeEdgeLabel("isLocatedIn").multiplicity(Multiplicity.MULTI).make();
        mgmt.makeEdgeLabel("isPartOf").multiplicity(Multiplicity.MULTI).make();
        mgmt.makeEdgeLabel("isSubclassOf").multiplicity(Multiplicity.MULTI).make();
        mgmt.makeEdgeLabel("likes").multiplicity(Multiplicity.MULTI).make();
        mgmt.makeEdgeLabel("knows").multiplicity(Multiplicity.MULTI).make();
        mgmt.makeEdgeLabel("replyOf").multiplicity(Multiplicity.MULTI).make();
        mgmt.makeEdgeLabel("studyAt").multiplicity(Multiplicity.MULTI).make();
        mgmt.makeEdgeLabel("workAt").multiplicity(Multiplicity.MULTI).make();
        mgmt.makeEdgeLabel("hasType").multiplicity(Multiplicity.MULTI).make();

        // define vertex property keys
        // TODO: Handle dates correctly
        mgmt.makePropertyKey("id").dataType(Long.class).cardinality(Cardinality.SINGLE).make();
        mgmt.makePropertyKey("title").dataType(String.class).cardinality(Cardinality.SINGLE).make();
//        mgmt.makePropertyKey("creationDate").dataType(Date.class).cardinality(Cardinality.SINGLE).make();
        mgmt.makePropertyKey("creationDate").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        mgmt.makePropertyKey("browserUsed").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        mgmt.makePropertyKey("locationIP").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        mgmt.makePropertyKey("content").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        mgmt.makePropertyKey("length").dataType(Integer.class).cardinality(Cardinality.SINGLE).make();
        mgmt.makePropertyKey("name").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        mgmt.makePropertyKey("url").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        mgmt.makePropertyKey("firstName").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        mgmt.makePropertyKey("lastName").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        mgmt.makePropertyKey("gender").dataType(String.class).cardinality(Cardinality.SINGLE).make();
//        mgmt.makePropertyKey("birthday").dataType(Date.class).cardinality(Cardinality.SINGLE).make();
        mgmt.makePropertyKey("birthday").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        mgmt.makePropertyKey("email").dataType(String.class).cardinality(Cardinality.SET).make();
        mgmt.makePropertyKey("language").dataType(String.class).cardinality(Cardinality.SET).make();
        mgmt.makePropertyKey("imageFile").dataType(String.class).cardinality(Cardinality.SINGLE).make();
        mgmt.makePropertyKey("type").dataType(String.class).cardinality(Cardinality.SINGLE).make();

        // define edge property keys
        mgmt.makePropertyKey("joinDate").dataType(Date.class).cardinality(Cardinality.SINGLE).make();
        mgmt.makePropertyKey("classYear").dataType(Integer.class).cardinality(Cardinality.SINGLE).make();
        mgmt.makePropertyKey("workFrom").dataType(Integer.class).cardinality(Cardinality.SINGLE).make();


        // define graph indexes
        // TODO: Configure external indexing backend to allow range scan of creation date properties on edges
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

        // create traversal source
        GraphTraversalSource g = graph.traversal();

        // get schema
        JanusGraphManagement schema = graph.openManagement();
        LOGGER.info(schema.printSchema());
        schema.commit();

        // properties that are Integer type
        List<String> integerProperties = new ArrayList<>();
        integerProperties.add("length");
        integerProperties.add("classYear");
        integerProperties.add("workForm");

        // properties that are DateTime type
        List<String> dateTimeProperties = new ArrayList<>();
        dateTimeProperties.add("joinDate");
//        dateTimeProperties.add("creationDate");

        // properties that are Set type
        List<String> setProperties = new ArrayList<>();
        setProperties.add("language");
        setProperties.add("email");

        // path to datagen directory
//        String datagenPath = System.getenv("LDBC_SNB_DATAGEN_HOME");

        String verticesPath = "/Users/jackwaudby/Documents/ldbc/ldbc_snb_interactive_validation/neo4j/neo4j--validation_set/social_network/string_date/vertices/";
        String edgesPath = "/Users/jackwaudby/Documents/ldbc/ldbc_snb_interactive_validation/neo4j/neo4j--validation_set/social_network/string_date/edges/";

//        String verticesPath = datagenPath + "/test_data/social_network/vertices/";
//        String edgesPath = datagenPath + "/test_data/social_network/edges/";

        LOGGER.info("Vertex Path: " + verticesPath);
        LOGGER.info("Edge Path: " + edgesPath);

        // ADD VERTICES

        // create directory
        File vertexDir = new File(verticesPath); // TODO: Can this step be performed with Apache Commons Library?
        // create array of files
        File[] vertexDirectoryListing = vertexDir.listFiles();
        // check directory is not empty
        if (vertexDirectoryListing != null) {
            // for each file in the directory
            for (File child : vertexDirectoryListing) {
                // ignore .crc files and update files
                if (!(child.toString().contains(".crc") || child.toString().contains("update") || child.toString().contains(".DS_Store"))) {


                    String [] tempArray = fileCleanUp(child.toString(),verticesPath);

                    // capitalise vertex label
                    String vertexLabel = tempArray[0];
                    vertexLabel = vertexLabel.substring(0, 1).toUpperCase() + vertexLabel.substring(1);

                    vertexLabel = tagClassFix(vertexLabel);

                    LOGGER.info("Adding Vertex: (" + vertexLabel+ ")");

                    // read file in
                    Reader in;
                    try {
                        // file
                        in = new FileReader(child);

                        // to iterate over records
                        Iterable<CSVRecord> records;
                        try {
                            records = CSVFormat.DEFAULT.withDelimiter('|').parse(in);

                            // get header
                            CSVRecord header = records.iterator().next();

                            // Add properties
                            for (CSVRecord record : records) {

                                Vertex v = g.addV(vertexLabel).property(header.get(0), Long.parseLong(record.get(0))).next();

                                // For each property in file
                                for (int i = 1; i < header.size(); i++) {
                                    if (integerProperties.contains(header.get(i))) { // Integer
                                        g.V(v).property(header.get(i), Integer.parseInt(record.get(i))).next();
                                    } else if (header.get(i).contentEquals("birthday1")) {
                                        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
                                        g.V(v).property(header.get(i), dateFormat.parse(record.get(i))).next();
                                    } else if (dateTimeProperties.contains(header.get(i))) { // Date
                                        SimpleDateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ"); // TODO: BST error
                                        g.V(v).property(header.get(i), dateTimeFormat.parse(record.get(i))).next();
                                    } else if (setProperties.contains(header.get(i))) {
                                        // parse string
                                        String delimiter2 = ":";
                                        String[] setArray = record.get(i).split(delimiter2);
                                        // add in loop
                                        for (int j = 0; j < setArray.length; j++) {

                                            g.V(v).property(VertexProperty.Cardinality.set, header.get(i), setArray[j]).next();
                                        }
                                    } else { // String

                                        g.V(v).property(header.get(i), record.get(i)).next();

                                    }
                                }
                                graph.tx().commit();

                            }
                        } catch (Exception e) {
                            System.out.println(e);
                        }
//                            graph.tx().commit(); TODO: Check if this is quicker

                    } catch (FileNotFoundException e) {
                            LOGGER.error(e);
                    }
                }
            }
        } else {
            LOGGER.error("Supplied path is not a directory");
        }

        // ADD EDGES

        // create directory
        File edgeDir = new File(edgesPath);
        // create array of files
        File[] edgeDirectoryListing = edgeDir.listFiles();

        // check directory is not empty
        if (edgeDirectoryListing != null) {
            // for each file in the directory
            for (File child : edgeDirectoryListing) {
                // ignore .crc files and update files
                if (!(child.toString().contains(".crc") || child.toString().contains("update") || child.toString().contains(".DS_Store"))) {


                    String[] tempArray = fileCleanUp(child.toString(),edgesPath);

                    String edgeTail = tempArray[0];
                    edgeTail = edgeTail.substring(0, 1).toUpperCase() + edgeTail.substring(1);
                    String edgeHead = tempArray[2];
                    edgeHead = edgeHead.substring(0, 1).toUpperCase() + edgeHead.substring(1);
                    String edgeLabel = tempArray[1];

                    edgeTail = tagClassFix(edgeTail);
                    edgeHead = tagClassFix(edgeHead);

                    LOGGER.info("Adding Edge: " + "(" + edgeTail + ")-[" + edgeLabel + "]->(" + edgeHead + ")");


                    // read file in
                    Reader in;
                    try {
                        // file
                        in = new FileReader(child);

                        // to iterate over records
                        Iterable<CSVRecord> records;
                        try {
                            // get record
                            records = CSVFormat.DEFAULT.withDelimiter('|').parse(in);
                            // get record header
                            CSVRecord header = records.iterator().next();

                            ArrayList<String> edgeInfo = new ArrayList<>();
                            for (int i = 0; i < header.size(); i++) {
                                edgeInfo.add(header.get(i));

                            }

                            for (CSVRecord record : records) {
                                g.V().has(edgeHead, "id", Long.parseLong(record.get(1))).as("a").V().has(edgeTail, "id", Long.parseLong(record.get(0))).addE(edgeLabel).to("a").next();
                            }

                            graph.tx().commit();

                        } catch (Exception e) {
                            System.out.println(e);
                        }
                    } catch (FileNotFoundException e) {
                        System.out.println(e);
                    }

                }
            }
        }


        // Graph stats
        GraphStats.elementCount(g);

        CloseGraph.closeGraph(g);

        System.exit(0);

    }

    // tag class fix
    static String tagClassFix(String s) {

        if (s.contentEquals("Tagclass")) {
            s = s.substring(0, 3) + s.substring(3, 4).toUpperCase() + s.substring(4);
        }

        return s;

    }


    // extracts labels from file names
    static String[] fileCleanUp(String s,String path) {

        // preprocess file, remove directory path and postfix
        String stringToSplit = s.toString();
        stringToSplit = stringToSplit.replace(path, "");
        stringToSplit = stringToSplit.replace("_0_0.csv", "");

        // split file name to get vertex and edge labels
        String delimiter = "_";
        String[] tempArray = stringToSplit.split(delimiter);

        return tempArray;

    }


}
