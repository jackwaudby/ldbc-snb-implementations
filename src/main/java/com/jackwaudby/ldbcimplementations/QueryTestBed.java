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

import static com.jackwaudby.ldbcimplementations.utils.HttpResponseToResultList.httpResponseToResultList;
import static com.jackwaudby.ldbcimplementations.utils.HttpResponseToResultMap.httpResponseToResultMap;

/**
 * Query test bed provides functionality for testing queries in a client-server model.
 * There are baseline queries for read vertex value
 */
public class QueryTestBed {
    public static void main(String[] args)  {

        // originalMessage=g.V().has('Post','id',34359748472).fold().coalesce(unfold(),V().has('Comment','id',34359748472)).next()
        // originalAuthor=g.V(originalMessage).out('hasCreator').next()
        // replies=g.V(originalMessage).in('replyOf').order().by('creationDate',desc).by('id',asc).toList()
        // repliesAuthor=g.V(replies).out('hasCreator').as('replyAuthor').toList()
        // g.V(repliesAuthor).as('repliesAuthor').choose(bothE('knows').otherV().hasId(originalAuthor.id()),constant(true),constant(false)).as('knows').select('knows','repliesAuthor')
        // g.V().has('Post','id',34359748472).fold().coalesce(unfold(),V().has('Comment','id',34359748472)).as('originalMessage').in('replyOf').as('comment').order().by(select('comment').by('creationDate'),desc).by('id',asc).out('hasCreator').as('replyAuthor').choose(bothE('knows').otherV().hasId(originalAuthor.id()),constant(true),constant(false)).as('knows').select('comment','replyAuthor','knows').by(valueMap('id','content','creationDate')).by(valueMap('id','firstName','lastName')).by()


        String queryString = "{\"gremlin\": \"" +                               // gremlin query string
                "originalMessage=g.V().has('Post','id',8590044640).fold().coalesce(unfold(),V().has('Comment','id',8590044640)).next();" +
                "originalAuthor=g.V(originalMessage).out('hasCreator').next();" +
                "g.V(originalMessage).as('originalMessage').in('replyOf').as('comment').order().by(select('comment').by('creationDate'),desc).by('id',asc).out('hasCreator').as('replyAuthor').choose(bothE('knows').otherV().hasId(originalAuthor.id()),constant(true),constant(false)).as('knows').select('comment','replyAuthor','knows').by(valueMap('id','content','creationDate')).by(valueMap('id','firstName','lastName')).by(fold());" +
                "\"" +
                "}";

        Map< String,String> hm = new HashMap<>();                               // init test bed
        hm.put("url","http://localhost:8182");
        JanusGraphDb x = new JanusGraphDb();
        x.init(hm);
        String response = x.execute(queryString);
        System.out.println(response);
        ArrayList<HashMap<String, String>> result                               // parse result
                = httpResponseToResultList(response);

        System.out.println(result);
//        int TX_ATTEMPTS = 0;
//        int TX_RETRIES = 5;
//        while (TX_ATTEMPTS < TX_RETRIES) {
//            System.out.println("Attempt " + (TX_ATTEMPTS + 1));
//            String response = x.execute(edgeCount);                                    // get response as string
//            System.out.println(response);                                               // print response string
//            HashMap<String, String> result = httpResponseToResultMap(response);         // convert to result map
//            if (result.containsKey("query_error")) {
//                TX_ATTEMPTS = TX_ATTEMPTS + 1;                                          // increment attempts
//                System.out.println("Query Error: " + result.get("query_error"));        // print error
//            } else if (result.containsKey("http_error")) {
//                TX_ATTEMPTS = TX_ATTEMPTS + 1;                                          // increment attempts
//                System.out.println("Gremlin Server Error: " + result.get("http_error"));// print error
//            } else {
//                System.out.println(result.toString());                                  // print parsed string
//                break;
//            }
//        }
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
