package com.jackwaudby.ldbcimplementations.utils;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * This script provides a method that converts http response into a result map
 */
public class HttpResponseToResultMap {

    // The response message consists of: requestId, status, result
    // Check code is 200, else return message body
    // If successful get the result
    // result consists of list of maps with property key and the values as a list
    public static HashMap<String, String> httpResponseToResultMap(String httpResponse){
        JSONObject responseJson = new JSONObject(httpResponse); // convert to JSON
//        System.out.println("HTTP Response Message: " + httpResponse);
        HashMap<String,String> resultMap = new HashMap<>();
        try {
            JSONObject status = responseJson.getJSONObject("status");               // get response status
//            System.out.println("Status: " + status);
            int statusCode = status.getInt("code");                             // get status code
//            System.out.println("Status code: " + statusCode);
            if (statusCode == 200) {                                                // if HTTP request successful
                JSONArray result = responseJson.getJSONObject("result")
                        .getJSONObject("data").getJSONArray("@value");          // get data
//                System.out.println("Result: " + result);
//                System.out.println("Elements in value map: " + result.length());

                for (int index = 0; index < result.length(); index++) {             // for each property in list
                    String elementKey = result.getJSONObject(index)
                            .getJSONArray("@value").getString(0);         // get property key
//                    System.out.println("Element Key: " + elementKey);
                    String elementValue;
                    try {                                                           // Date/Integer JSON path
                        JSONObject testObject = result.getJSONObject(index)
                                .getJSONArray("@value").getJSONObject(1)
                                .getJSONArray("@value").getJSONObject(0);
                        elementValue = testObject.get("@value").toString();
//                        System.out.println("Element Value: " + elementValue);
                    } catch (JSONException e) {                                     // Set/String JSON path
                        int elementValueSize = result.getJSONObject(index)
                                .getJSONArray("@value").getJSONObject(1).
                                        getJSONArray("@value").length();
                        if (elementValueSize == 1) {                                // String
                            elementValue = result.getJSONObject(index)
                                    .getJSONArray("@value").getJSONObject(1)
                                    .getJSONArray("@value").getString(0);
//                            System.out.println("Element Value: " + elementValue);
                        } else {                                                    // Set
                            ArrayList<String> elementValueSet = new ArrayList<>();
                            for (int i = 0; i < elementValueSize; i++) {
                                elementValueSet.add(result.getJSONObject(index)
                                        .getJSONArray("@value").getJSONObject(1)
                                        .getJSONArray("@value").getString(i));
                            }
                            elementValue = elementValueSet.toString();
//                            System.out.println("Element Value: " + elementValueSet.toString());
                        }
                    }
                    resultMap.put(elementKey, elementValue);                        // add to result map
                }
//                System.out.println(resultMap.toString());
            } else {                                                                // return error message
                String statusMessage = status.getString("message");
                System.out.println("Status message: " + statusMessage);
                resultMap.put("http_error",statusMessage);
            }
        } catch (JSONException e){
            e.printStackTrace();
        }

        return resultMap;

    }
}
