package com.jackwaudby.ldbcimplementations.utils;

import com.jackwaudby.ldbcimplementations.CompleteLoader;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.janusgraph.core.JanusGraph;
import java.io.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.TimeZone;


import static com.jackwaudby.ldbcimplementations.utils.ExtractLabels.extractLabels;
import static com.jackwaudby.ldbcimplementations.utils.LineCount.lineCount;
import static com.jackwaudby.ldbcimplementations.utils.TagClassFix.tagClassFix;

/**
 * This script provides a method for loading vertices.
 */
public class BulkLoadVertices {

    public static void bulkLoadVertices(String pathToData, JanusGraph graph, GraphTraversalSource g, HashMap<String, Object> ldbcIdToJanusGraphId) {


        List<String> integerProperties = new ArrayList<>();                     // properties that are Integer type
        integerProperties.add("length");
        integerProperties.add("classYear");
        integerProperties.add("workForm");
        List<String> dateTimeProperties = new ArrayList<>();                    // properties that are DateTime type
        dateTimeProperties.add("joinDate");
        dateTimeProperties.add("creationDate");
        List<String> setProperties = new ArrayList<>();                         // properties that are Set type
        setProperties.add("language");
        setProperties.add("email");

        String validVertexFiles = "comment_0_0.csv,forum_0_0.csv," +
                "person_0_0.csv,organisation_0_0.csv,place_0_0.csv," +
                "post_0_0.csv,tag_0_0.csv,tagclass_0_0.csv";                    // vertex files
        String[] vertexFileNames = validVertexFiles.split(",");
        List<String> vertexFilePaths = new ArrayList<>();                       // valid vertex file paths
        for (int i =0; i<vertexFileNames.length;i++){
            vertexFilePaths.add(i,pathToData + vertexFileNames[i]);
        }

        File dataDirectory = new File(pathToData);                              // create directory
        File[] filesInDataDirectory = dataDirectory.listFiles();                // create array of files
        if (filesInDataDirectory != null) {                                     // check directory is not empty
            for (File child : filesInDataDirectory) {                           // for each file in the directory
                if (!(child.toString().contains(".crc") ||                      // ignore .crc files
                        child.toString().contains("update") ||                  // ignore update files
                        child.toString().contains(".DS_Store")) &&              // ignore DS
                        (vertexFilePaths.contains(child.toString()))) {         // valid vertex path

                    String[] cleanFileName = extractLabels(child.toString(), pathToData);   // clean file name
                    String vertexLabel = cleanFileName[0];                                  // capitalise vertex label
                    vertexLabel = vertexLabel.substring(0, 1).toUpperCase() + vertexLabel.substring(1);
                    vertexLabel = tagClassFix(vertexLabel);                                 // tag class fix

                    CompleteLoader.LOGGER.info("Adding Vertex: (" + vertexLabel + ")");

                    int elementsToAdd = lineCount(child); // number of elements to add

                    Reader in;
                    try {
                        in = new FileReader(child); // read file
                        Iterable<CSVRecord> records; // to iterate over records
                        try {
                            records = CSVFormat.DEFAULT.withDelimiter('|').parse(in);   // break into lines
                            CSVRecord header = records.iterator().next();               // get header

                            int elementsAdded = 0;
                            int elementsExist = 0;
                            for (CSVRecord record : records) { // for each record in file

                                String compositeLdbcId = record.get(0) + vertexLabel;
                                long ldbcId = Long.parseLong(record.get(0));

                                Vertex v = g.addV(vertexLabel).property(header.get(0),ldbcId).next(); // add vertex to graph

                                ldbcIdToJanusGraphId.put(compositeLdbcId,v.id()); // add to id map

                                    elementsAdded = elementsAdded + 1; // increment elements added

                                    for (int i = 1; i < header.size(); i++) { // add properties to vertex
                                        if (integerProperties.contains(header.get(i))) { // Integer
                                            g.V(v).property(header.get(i), Integer.parseInt(record.get(i))).next();
                                        } else if (header.get(i).contentEquals("birthday")) { // Date
                                            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
                                            dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
                                            g.V(v).property(header.get(i), dateFormat.parse(record.get(i))).next();
                                        } else if (dateTimeProperties.contains(header.get(i))) { // DateTime
                                            SimpleDateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
                                            g.V(v).property(header.get(i), dateTimeFormat.parse(record.get(i))).next();
                                        } else if (setProperties.contains(header.get(i))) { // Set
                                            String delimiter2 = ";"; // parse string CHECK THIS IS RIGHT either ; or : //TODO change to ; in preprocessing script so consistent across all
                                            String[] setArray = record.get(i).split(delimiter2);
                                            for (String s : setArray) { // add in loop
                                                g.V(v).property(VertexProperty.Cardinality.list, header.get(i), s).next();
                                            }
                                        } else { // String
                                            g.V(v).property(header.get(i), record.get(i)).next();
                                        }
                                    }
                                    graph.tx().commit(); // commit vertex
                            }
                            CompleteLoader.LOGGER.info((elementsAdded + elementsExist) +"/" + elementsToAdd);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    } catch (IOException e) {
                        CompleteLoader.LOGGER.error(e);
                    }
                }
            }
        } else {
            CompleteLoader.LOGGER.error("Supplied path is not a directory");
        }
        graph.tx().commit();
    }
}
