package com.jackwaudby.ldbcimplementations;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import static com.jackwaudby.ldbcimplementations.utils.HttpResponseToResultMap.httpResponseToResultMap;

public class QueryTestBed {
    public static void main(String[] args)  {

//        printOperation("Update1AddPerson");

        //        Query 1
        //        valid: 1099511630063
        //        not valid:  68

        String readQuery1 = "{\"gremlin\": \"" +
                "try { " +
                "v = g.V().has('Person','id',5497558139615).next();[];" +
                "hm = g.V(v).valueMap('id','firstName','lastName','birthday','locationIP','browserUsed','gender','creationDate','language','email').next();[];" +
                "v2 = g.V(v).outE('isLocatedIn').inV().valueMap('id').next();[];" +
                "cityId = v2['id'];[];" +
                "hm.put('cityId',cityId);[];" +
                "graph.tx().commit();[];" +
                "hm;" +
                "} catch (Exception e) {" +
                "errorMessage =[e.toString()];[];" +
                "hm=[query_error:errorMessage];[];" +
                "graph.tx().rollback();[];" +
                "};" +
                "hm;\"" +
                "}";

        //        Query 4
        String readQuery4 =  "{\"gremlin\": \"post_exists = g.V().has('Post','id',42949783512).hasNext();[];" +
                "if(post_exists){" +
                "v=g.V().has('Post','id',42949783512).valueMap('creationDate','content','imageFile').next();[]" +
                "} else {" +
                "v=g.V().has('Comment','id',42949783512).valueMap('creationDate','content','imageFile').next();[]" +
                "};" +
                "v\"}";

//        Update
        String update1 = "{\"gremlin\": \"" +
                "try {" +
                "p = g.addV('Person').property('id',99).property('firstName','John').property('lastName','Doe')" +
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
//                "tagid=[139, 205, 286, 470, 538];[];"+
//                "for (item in tagid) { " +
//                "g.V().has('Tag', 'id', item).as('tag').V(p).addE('hasInterest').to('tag').next();[];" +
//                "};"+
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

        String delete  = "{\"gremlin\": \"g.V().has('Person','id',5497558139615).drop()\"}";



        Map< String,String> hm = new HashMap<>();                               // init test bed
        hm.put("url","http://localhost:8182");
        JanusGraphDb x = new JanusGraphDb();
        x.init(hm);

        int TX_ATTEMPTS = 0;
        int TX_RETRIES = 5;
        while (TX_ATTEMPTS < TX_RETRIES) {
            System.out.println("Attempt " + (TX_ATTEMPTS + 1));
            String response = x.execute(delete);                                // get response as string
            HashMap<String, String> result = httpResponseToResultMap(response);      // convert to result map
            if (result.containsKey("query_error")) {
                TX_ATTEMPTS = TX_ATTEMPTS + 1;
                System.out.println("Query Error: " + result.get("query_error"));
            } else if (result.containsKey("http_error")) {
                TX_ATTEMPTS = TX_ATTEMPTS + 1;
                System.out.println("Gremlin Server Error: " + result.get("http_error"));
            } else {
                // create result object
                System.out.println(result.toString());
                break;
            }
        }


        x.onClose();                                                            // close test bed








    }
    // A method for converting the response from valueMap() a Gremlin method to a Map <PropertyKey, PropertyValue>
//    // The response message consists of: requestId, status, result
//    // Check code is 200, else return message body
//    // If successful get the result
//    // result consists of list of maps with property key and the values as a list
//    public static HashMap<String, String> httpResponseToResultMap(String httpResponse){
//        JSONObject responseJson = new JSONObject(httpResponse); // convert to JSON
//        System.out.println("HTTP Response Message: " + httpResponse);
//        HashMap<String,String> resultMap = new HashMap<>();
//        try {
//            JSONObject status = responseJson.getJSONObject("status");               // get response status
////            System.out.println("Status: " + status);
//            int statusCode = status.getInt("code");                             // get status code
//            System.out.println("Status code: " + statusCode);
//            if (statusCode == 200) {                                                // if HTTP request successful
//                JSONArray result = responseJson.getJSONObject("result")
//                        .getJSONObject("data").getJSONArray("@value");          // get data
////                System.out.println("Result: " + result);
////                System.out.println("Elements in value map: " + result.length());
//
//                for (int index = 0; index < result.length(); index++) {             // for each property in list
//                    String elementKey = result.getJSONObject(index)
//                            .getJSONArray("@value").getString(0);         // get property key
////                    System.out.println("Element Key: " + elementKey);
//                    String elementValue;
//                    try {                                                           // Date/Integer JSON path
//                        JSONObject testObject = result.getJSONObject(index)
//                                .getJSONArray("@value").getJSONObject(1)
//                                .getJSONArray("@value").getJSONObject(0);
//                        elementValue = testObject.get("@value").toString();
////                        System.out.println("Element Value: " + elementValue);
//                    } catch (JSONException e) {                                     // Set/String JSON path
//                        int elementValueSize = result.getJSONObject(index)
//                                .getJSONArray("@value").getJSONObject(1).
//                                        getJSONArray("@value").length();
//                        if (elementValueSize == 1) {                                // String
//                            elementValue = result.getJSONObject(index)
//                                    .getJSONArray("@value").getJSONObject(1)
//                                    .getJSONArray("@value").getString(0);
////                            System.out.println("Element Value: " + elementValue);
//                        } else {                                                    // Set
//                            ArrayList<String> elementValueSet = new ArrayList<>();
//                            for (int i = 0; i < elementValueSize; i++) {
//                                elementValueSet.add(result.getJSONObject(index)
//                                        .getJSONArray("@value").getJSONObject(1)
//                                        .getJSONArray("@value").getString(i));
//                            }
//                            elementValue = elementValueSet.toString();
////                            System.out.println("Element Value: " + elementValueSet.toString());
//                        }
//                    }
//                    resultMap.put(elementKey, elementValue);                        // add to result map
//                }
////                System.out.println(resultMap.toString());
//            } else {                                                                // return error message
//                String statusMessage = status.getString("message");
//                System.out.println("Status message: " + statusMessage);
//                resultMap.put("http_error",statusMessage);
//            }
//        } catch (JSONException e){
//            e.printStackTrace();
//        }
//
//        return resultMap;
//
//    }

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
