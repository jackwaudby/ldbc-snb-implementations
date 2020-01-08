package com.jackwaudby.ldbcimplementations.utils;

import com.jackwaudby.ldbcimplementations.CompleteLoader;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.janusgraph.core.JanusGraph;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.Reader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import static com.jackwaudby.ldbcimplementations.utils.ExtractLabels.extractLabels;
import static com.jackwaudby.ldbcimplementations.utils.LineCount.lineCount;
import static com.jackwaudby.ldbcimplementations.utils.TagClassFix.tagClassFix;

/**
 * This script provides a method for bulk loading edges.
 */
public class BulkLoadEdges {

    public static void bulkLoadEdges(String pathToData, JanusGraph graph, GraphTraversalSource g, HashMap<String, Object> ldbcIdToJanusGraphId) {


        List<String> integerProperties = new ArrayList<>();                                 // edge property types
        integerProperties.add("classYear");
        integerProperties.add("workFrom");

        String validVertexFiles = "comment_0_0.csv,forum_0_0.csv,person_0_0.csv," +         // vertex files
                "organisation_0_0.csv,place_0_0.csv," +                                     // used to distinguish
                "post_0_0.csv,tag_0_0.csv,tagclass_0_0.csv";                                // edges files
        String[] vertexFileNames = validVertexFiles.split(",");
        List<String> vertexFilePaths = new ArrayList<>();                                   // vertex file paths
        for (int i = 0; i < vertexFileNames.length; i++) {
            vertexFilePaths.add(i, pathToData + vertexFileNames[i]);
        }

        File dataDirectory = new File(pathToData);                                  // create directory
        File[] filesInDataDirectory = dataDirectory.listFiles();                    // create array of files
        if (filesInDataDirectory != null) {                                         // check directory is not empty
            for (File file : filesInDataDirectory) {                                // for each file in the directory
                if (!(file.toString().contains(".crc") ||                           // ignore .crc files
                        file.toString().contains("update") ||                       // ignore update files
                        file.toString().contains(".DS_Store")) &&                   // ignore DS
                        (!vertexFilePaths.contains(file.toString()))) {             // not a vertex file

                    String[] cleanFileName = extractLabels(file.toString(), pathToData);        // get edge labels
                    String edgeTail = cleanFileName[0];                                         // edge tail vertex
                    edgeTail = edgeTail.substring(0, 1).toUpperCase() + edgeTail.substring(1);  // capitalise
                    String edgeHead = cleanFileName[2];                                         // edge head vertex
                    edgeHead = edgeHead.substring(0, 1).toUpperCase() + edgeHead.substring(1);  // capitalise
                    String edgeLabel = cleanFileName[1];                                        // edge label
                    edgeTail = tagClassFix(edgeTail);                                           // check for tag class fix
                    edgeHead = tagClassFix(edgeHead);                                           // check for tag class fix

                    CompleteLoader.LOGGER.info("Adding Edge: " + "(" + edgeTail + ")-" +
                            "[:" + edgeLabel + "]->(" + edgeHead + ")");

                    int elementsToAdd = lineCount(file);                                        // elements to add

                    Reader in;                                                                  // read file in
                    try {
                        in = new FileReader(file);                                              // file
                        Iterable<CSVRecord> records;                                            // to iterate over records
                        try {
                            records = CSVFormat.DEFAULT.withDelimiter('|').parse(in);           // get records
                            CSVRecord header = records.iterator().next();                       // get record header

                            ArrayList<String> edgeInfo = new ArrayList<>();
                            for (int i = 0; i < header.size(); i++) {                           // edge information
                                edgeInfo.add(header.get(i));
                            }

                            int elementsAdded = 0;
                            String startId;
                            String endId;
                            if (edgeInfo.size() == 3) {                                         // if edge has property

                                String edgePropertyKey = edgeInfo.get(2);

                                for (CSVRecord record : records) {

                                    elementsAdded = elementsAdded + 1;                          // increment elements added
                                    startId = record.get(0) + edgeTail;                  // keys to look up internal id
                                    endId = record.get(1) + edgeHead;

                                    if (integerProperties.contains(edgePropertyKey)) {           // edge property is int

                                        long edgePropertyValue = Long.parseLong(record.get(2));

                                        g.V().hasId(ldbcIdToJanusGraphId.get(endId)).as("a")
                                                .V().hasId(ldbcIdToJanusGraphId.get(startId))
                                                .addE(edgeLabel)
                                                .property(edgePropertyKey, edgePropertyValue)
                                                .to("a")
                                                .next();

                                    } else {                                                    // edge property is date

                                        SimpleDateFormat dateTimeFormat =
                                                new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
                                        Date edgePropertyValue = dateTimeFormat.parse(record.get(2));

                                        g.V().hasId(ldbcIdToJanusGraphId.get(endId)).as("a")
                                                .V().hasId(ldbcIdToJanusGraphId.get(startId))
                                                .addE(edgeLabel)
                                                .property(edgePropertyKey, edgePropertyValue.getTime())
                                                .to("a")
                                                .next();

                                    }
                                }
                            } else {
                                for (CSVRecord record : records) {
                                    elementsAdded = elementsAdded + 1;

                                    startId = record.get(0) + edgeTail;
                                    endId = record.get(1) + edgeHead;

                                    g.V().hasId(ldbcIdToJanusGraphId.get(endId)).as("a")
                                            .V().hasId(ldbcIdToJanusGraphId.get(startId))
                                            .addE(edgeLabel)
                                            .to("a")
                                            .next();

                                }
                            }

                            graph.tx().commit();

                            CompleteLoader.LOGGER.info(elementsAdded + "/" + elementsToAdd);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    } catch (FileNotFoundException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        graph.tx().commit();
    }
}

