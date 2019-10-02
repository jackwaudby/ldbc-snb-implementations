package com.jackwaudby.ldbcimplementations.utils;

import com.jackwaudby.ldbcimplementations.GraphLoader;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.janusgraph.core.JanusGraph;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

import static com.jackwaudby.ldbcimplementations.utils.ExtractLabels.extractLabels;
import static com.jackwaudby.ldbcimplementations.utils.LineCount.lineCount;
import static com.jackwaudby.ldbcimplementations.utils.TagClassFix.tagClassFix;

/**
 * This script provides a method for loading edges.
 */
public class BulkLoadEdges {

    public static void bulkLoadEdges(String pathToData, JanusGraph graph, GraphTraversalSource g) {

        // duplicated between both load classes
        List<String> integerProperties = new ArrayList<>();
        integerProperties.add("length");
        integerProperties.add("classYear");
        integerProperties.add("workForm");
        List<String> dateTimeProperties = new ArrayList<>();
        dateTimeProperties.add("joinDate");
        dateTimeProperties.add("creationDate");
        List<String> setProperties = new ArrayList<>();
        setProperties.add("language");
        setProperties.add("email");

        String validVertexFiles = "comment_0_0.csv,forum_0_0.csv,person_0_0.csv,organisation_0_0.csv,place_0_0.csv,post_0_0.csv,tag_0_0.csv,tagclass_0_0.csv"; // vertex files, used to work which files to ignore
        String[] vertexFileNames = validVertexFiles.split(",");
        List<String> vertexFilePaths = new ArrayList<>(); // valid vertex file paths
        for (int i = 0; i < vertexFileNames.length; i++) {
            vertexFilePaths.add(i, pathToData + vertexFileNames[i]);
        }

        File dataDirectory = new File(pathToData); // create directory
        File[] filesInDataDirectory = dataDirectory.listFiles(); // create array of files
        if (filesInDataDirectory != null) { // check directory is not empty
            for (File file : filesInDataDirectory) { // for each file in the directory
                if (!(file.toString().contains(".crc") || // ignore .crc files
                        file.toString().contains("update") || // ignore update files
                        file.toString().contains(".DS_Store")) &&  // ignore DS
                        (!vertexFilePaths.contains(file.toString()))) { // valid vertex path
                    String[] cleanFileName = extractLabels(file.toString(), pathToData); // get edge label, tail and head vertex
                    String edgeTail = cleanFileName[0];  // edge tail vertex
                    edgeTail = edgeTail.substring(0, 1).toUpperCase() + edgeTail.substring(1); // capitalise
                    String edgeHead = cleanFileName[2]; // edge head vertex
                    edgeHead = edgeHead.substring(0, 1).toUpperCase() + edgeHead.substring(1); // capitalise
                    String edgeLabel = cleanFileName[1]; // edge label
                    edgeTail = tagClassFix(edgeTail); // check for tag class fix
                    edgeHead = tagClassFix(edgeHead); // check for tag class fix
                    GraphLoader.LOGGER.info("Adding Edge: " + "(" + edgeTail + ")-[:" + edgeLabel + "]->(" + edgeHead + ")");
                    int elementsToAdd = lineCount(file); // elements to add
                    Reader in; // read file in
                    try {
                        in = new FileReader(file); // file
                        Iterable<CSVRecord> records; // to iterate over records
                        try {
                            records = CSVFormat.DEFAULT.withDelimiter('|').parse(in); // get records
                            CSVRecord header = records.iterator().next(); // get record header
                            ArrayList<String> edgeInfo = new ArrayList<>();
                            for (int i = 0; i < header.size(); i++) { // edge information
                                edgeInfo.add(header.get(i));
                            }
                            int elementsAdded = 0;
                            for (CSVRecord record : records) {
                                elementsAdded = elementsAdded + 1; // increment elements added
                                //System.out.print("Progress: " + elementsAdded + "/" + elementsToAdd + "\r");
                                g.V().has(edgeHead, "id", Long.parseLong(record.get(1))).as("a").V().has(edgeTail, "id", Long.parseLong(record.get(0))).addE(edgeLabel).to("a").next(); // add edge
                            }
                            graph.tx().commit(); // commit edges
                            GraphLoader.LOGGER.info(elementsAdded + "/" + elementsToAdd);
                        } catch (Exception e) {
                            e.printStackTrace();                        }
                    } catch (FileNotFoundException e) {
                        e.printStackTrace();                    }
                }
            }
        }
    }
}

