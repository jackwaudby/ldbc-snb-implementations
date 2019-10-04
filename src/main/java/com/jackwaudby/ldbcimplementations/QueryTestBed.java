package com.jackwaudby.ldbcimplementations;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate1AddPerson;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate7AddComment;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import static com.jackwaudby.ldbcimplementations.utils.HttpResponseToResultMap.httpResponseToResultMap;

/**
 * Query test bed provides functionality for testing queries in a client-server model.
 * There are baseline queries for read vertex value
 */
public class QueryTestBed {
    public static void main(String[] args)  {

        // parameters
        long commentId = 3;
        long creationDate = 10000000;
        String locationIp = "12.12.12.12";
        String browserUsed = "Safari";
        String content = "woo pies";
        int length = 8;

        long authorPersonId = 6597069766681L;
        long countryId = 0;
        long replyToPostId = 343597383680L;
        long replyToCommentId = -1;

        List<Long> tagIds = new ArrayList<>();
        tagIds.add(30L);
        tagIds.add(60L);
        tagIds.add(80L);

        // add vertex and associated edges template
        String addVertex = "{\"gremlin\": \"" +
                "try {" +
                "p = g.addV('Comment').property('id'," +
                commentId +
                ").property('creationDate'," +
                creationDate +
                ").property('locationIP','" +
                locationIp +
                "')" +
                ".property('browserUsed','" +
                browserUsed +
                "').property('content','" +
                content +
                "')" +
                ".property('length'," +
                length +
                ").next();[];" +
                "g.V().has('Person', 'id'," +
                authorPersonId +
                ").as('creator').V(p).addE('hasCreator').to('creator').next();[];" +
                "g.V().has('Place', 'id'," +
                countryId +
                ").as('location').V(p).addE('isLocatedIn').to('location').next();[];" +
                "tagid=" +
                tagIds.toString() +
                ";[];"+
                "for (item in tagid) { " +
                "g.V().has('Tag', 'id', item).as('tag').V(p).addE('hasTag').to('tag').next();[];" +
                "};" +
                "if (" + replyToPostId + "==-1){" +
                "g.V().has('Comment', 'id'," + replyToCommentId + ").as('comment').V(p).addE('replyOf').to('comment').next();[];" +
                "} else {" +
                "g.V().has('Post', 'id'," + replyToPostId + ").as('post').V(p).addE('replyOf').to('post').next();[];" +
                "};" +
                "graph.tx().commit();[];" +
                "queryOutcome=['success'];[];" +
                "hm=[query_outcome:queryOutcome];[];" +
                "} catch (Exception e) {" +
                "errorMessage =[e.toString()];[];" +
                "hm=[query_error:errorMessage];[];" +
                "graph.tx().rollback();[];" +
                "};" +
                "hm;\"" +
                "}";


        // read vertex properties template
        String readVertex = "{\"gremlin\": \"" +
                "try { " +
                "v = g.V().has('Comment','id'," +
                commentId +
                ").next();[];" +
                "result = g.V(v).valueMap('id','imageFile','creationDate','locationIP','browserUsed','language','content','length').next();[];" +
//                "v2 = g.V(v).outE('hasCreator').inV().valueMap('id').next();[];" +
//                "authorId = v2['id'];[];" +
//                "result.put('authorId',authorId);[];" +
//                "v3 = g.V(v).outE('isLocatedIn').inV().valueMap('id').next();[];" +
//                "locationId = v3['id'];[];" +
//                "result.put('locationId',locationId);[];" +
//                "v4 = g.V(v).outE('replyOf').inV().valueMap('id').next();[];" +
//                "replyToId = v4['id'];[];" +
//                "result.put('replyToId',replyToId);[];" +
                "graph.tx().commit();[];" +
                "result;" +
                "} catch (Exception e) {" +
                "errorMessage =[e.toString()];[];" +
                "result=[query_error:errorMessage];[];" +
                "graph.tx().rollback();[];" +
                "};" +
                "result;\"" +
                "}";

        // delete vertex template
        String deleteVertex  = "{\"gremlin\": \"" +
                "try {" +
                "g.V().has('Comment','id'," + commentId + ").drop();[];" +
                "graph.tx().commit();[];" +
                "queryOutcome=['success'];[];" +
                "hm=[query_outcome:queryOutcome];[];" +
                "} catch (Exception e) {" +
                "errorMessage =[e.toString()];[];" +
                "hm=[query_error:errorMessage];[];" +
                "graph.tx().rollback();[];" +
                "};" +
                "hm;\"" +
                "}";

        String deleteVertex2  = "{\"gremlin\": \"" +
                "g.V().has('Comment','id'," + commentId + ").drop()" +
                "\"" +
                "}";

        // vertex count
        String vertexCount = "{\"gremlin\": \"" +
                "g.V().hasLabel('Comment').count()" +
                "\"" +
                "}";
        // edge count
        String edgeCount = "{\"gremlin\": \"" +
                "g.E().count()" +
                "\"" +
                "}";

        Map< String,String> hm = new HashMap<>();                               // init test bed
        hm.put("url","http://localhost:8182");
        JanusGraphDb x = new JanusGraphDb();
        x.init(hm);
        // 67
        // 31849
        int TX_ATTEMPTS = 0;
        int TX_RETRIES = 5;
        while (TX_ATTEMPTS < TX_RETRIES) {
            System.out.println("Attempt " + (TX_ATTEMPTS + 1));
            String response = x.execute(edgeCount);                                    // get response as string
            System.out.println(response);                                               // print response string
            HashMap<String, String> result = httpResponseToResultMap(response);         // convert to result map
            if (result.containsKey("query_error")) {
                TX_ATTEMPTS = TX_ATTEMPTS + 1;                                          // increment attempts
                System.out.println("Query Error: " + result.get("query_error"));        // print error
            } else if (result.containsKey("http_error")) {
                TX_ATTEMPTS = TX_ATTEMPTS + 1;                                          // increment attempts
                System.out.println("Gremlin Server Error: " + result.get("http_error"));// print error
            } else {
                System.out.println(result.toString());                                  // print parsed string
                break;
            }
        }
        x.onClose();                                                                    // close test bed
    }


    public static String extractData(String httpResponse) {

        JSONObject jsonObject = new JSONObject(httpResponse);
        String result=null;
        try {
            result = jsonObject.getJSONObject("result").getJSONObject("data").getJSONArray("@value").getString(0);
        } catch (JSONException e){
            e.printStackTrace();
        }
        return result;
    }

    static void printOperation(String operationName) {
        String pathToCsv = "/Users/jackwaudby/Documents/janusgraph/validation/validation_params_subset.csv";
        try {
            BufferedReader csvReader = new BufferedReader(new FileReader(pathToCsv));
            String row;
            while ((row = csvReader.readLine()) != null) {
                String[] aRow = row.split("\\|"); // splits into query | result
                String[] typeParam = aRow[0].split(","); // splits query on ,
                String operationType = typeParam[0].replaceAll("\"", "") // gets query type
                        .replace("[com.ldbc.driver.workloads.ldbc.snb.interactive.Ldbc", "");
                ArrayList<String> operationParams = new ArrayList<>();
                for (int i = 1; i < typeParam.length; i++) { // gets query params
                    operationParams.add(typeParam[i].replaceAll("\",", ""));
                }
                String[] queryResult = aRow[1].replaceAll("\",", "|").replaceAll(",\"", "|").replaceAll("[\\[\\]\"]", "").split("\\|");


                if (operationType.contains(operationName)) {
                    System.out.println("Query Type: " + operationType);
                    System.out.println("Query Parameters: " + operationParams);
                    System.out.println("Query Result: " + Arrays.toString(queryResult));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
