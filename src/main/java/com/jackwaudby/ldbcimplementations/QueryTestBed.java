package com.jackwaudby.ldbcimplementations;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate1AddPerson;
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
        long personId = 1099511630063L;
        List<Long> tagIds = new ArrayList<>();
        tagIds.add(30L);
        tagIds.add(60L);
        tagIds.add(80L);

        // read vertex properties template
        String readVertex = "{\"gremlin\": \"" +
                "try { " +
                "v = g.V().has('Person','id'," +
                personId +
                ").next();[];" +
                "result = g.V(v).valueMap('id','firstName','lastName','birthday','locationIP','browserUsed','gender','creationDate','language','email').next();[];" +
                "v2 = g.V(v).outE('isLocatedIn').inV().valueMap('id').next();[];" +
                "cityId = v2['id'];[];" +
                "result.put('cityId',cityId);[];" +
                "graph.tx().commit();[];" +
                "result;" +
                "} catch (Exception e) {" +
                "errorMessage =[e.toString()];[];" +
                "result=[query_error:errorMessage];[];" +
                "graph.tx().rollback();[];" +
                "};" +
                "result;\"" +
                "}";


        // add vertex and associated edges template
        String addVertex = "{\"gremlin\": \"" +
                "try {" +
                "p = g.addV('Person').property('id'," +
                personId +
                ").property('firstName','John').property('lastName','Doe')" +
                ".property('gender','female').property('birthday',789999).property('creationDate',789999)" +
                ".property('locationIP','56,89.567.445').property('browserUsed','Safari').next();[];" +
                "g.V().has('Place', 'id', 526).as('city').V(p).addE('isLocatedIn').to('city').next();[];" +
                "languages=['en','de'];[];"+
                "for (item in languages) { " +
                " g.V(p).property(set, 'language', item).next();[];" +
                "}; "+
                "email=['jack726@hotmail.com','jack726@icloud.com'];[];"+
                "for (item in email) { " +
                " g.V(p).property(set, 'email', item).next();[];" +
                "}; "+
                "tagid=" +
                tagIds.toString() +
                ";[];"+
                "for (item in tagid) { " +
                "g.V().has('Tag', 'id', item).as('tag').V(p).addE('hasInterest').to('tag').next();[];" +
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

        // delete vertex template
        String deleteVertex  = "{\"gremlin\": \"" +
                "try {" +
                "g.V().has('Person','id'," + personId + ").drop()" +
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

        // read edge template
        String readEdge = "{\"gremlin\": \"" +
                "g.V().has('Person','id'," +
                personId +
                ").outE('knows').count()" +
                "\"" +
                "}";

        Map< String,String> hm = new HashMap<>();                               // init test bed
        hm.put("url","http://localhost:8182");
        JanusGraphDb x = new JanusGraphDb();
        x.init(hm);

        int TX_ATTEMPTS = 0;
        int TX_RETRIES = 5;
        while (TX_ATTEMPTS < TX_RETRIES) {
            System.out.println("Attempt " + (TX_ATTEMPTS + 1));
            String response = x.execute(readEdge);                                    // get response as string
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
