package com.jackwaudby.ldbcimplementations.utils;

import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class HttpResponseToResultList {



    // Multiple results stored in a list, each list contains a map (path element key=path element value),
    // each path element value contains a map (property key=property value),
    // each property value is a list

    // The path element values for each path element key are combined into a hash map
    // The hashmash are stored in an array list

    public static ArrayList<HashMap<String, String>> httpResponseToResultList(String httpResponse) {

        JSONObject responseJson = new JSONObject(httpResponse);                             // convert to JSON
        JSONArray results = responseJson.getJSONObject("result")                        // get results
                .getJSONObject("data").getJSONArray("@value");
        ArrayList<HashMap<String, String>> resultList = new ArrayList<>();              // init result arraylist
        for (int i = 0; i < results.length(); i++) {                                     // for each result
            JSONArray result = results.getJSONObject(i).getJSONArray("@value");
            HashMap<String, String> propertyPair = new HashMap<>();                     // init hashmap
            for (int j = 0; j < result.length(); j++) {                                  // for each path element
                String pathElementKey = result.get(j).toString();                       // get path element key
                j = j + 1;
                JSONArray pathElementValue =
                        result.getJSONObject(j).getJSONArray("@value");             // get path element value
                for (int k = 0; k < pathElementValue.length(); k++) {                     // for each property in path element value
                    String propertyKey = pathElementValue.get(k).toString();             // get property key
                    if (propertyKey.equals("id")) {                                      // multiple path element can have same property key - break on path element name
                        propertyKey = pathElementKey +
                                propertyKey.substring(0, 1).toUpperCase() + propertyKey.substring(1);
                    }
                    k = k + 1;
                    String propertyValue;
                    try {                                                                // get property value, either String or JSONObject (converted to String)
                        propertyValue = pathElementValue.getJSONObject(k)
                                .getJSONArray("@value").getJSONObject(0).get("@value").toString();
                    } catch (JSONException e) {
                        propertyValue = pathElementValue
                                .getJSONObject(k).getJSONArray("@value").getString(0);
                    }
                    propertyPair.put(propertyKey, propertyValue);                        // put in hashmap
                }
            }
            resultList.add(propertyPair);                                               // add hashmap for result in arraylist
        }
        return resultList;
    }
}
