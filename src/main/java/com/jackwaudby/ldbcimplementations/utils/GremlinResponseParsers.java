package com.jackwaudby.ldbcimplementations.utils;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * This class includes a number of methods for parsing HTTP responses from the Gremlin Server.
 * They are written in a manner to allow composition.
 */
public class GremlinResponseParsers {

    /**
     * This method converts a Gremlin list into a Java ArrayList of JSONObjects.
     * @param list JSONObject containing a JSONArray (list)
     * @return ArrayList of JSONObjects
     */
    static public ArrayList<JSONObject> gremlinListToArrayList (JSONObject list) {

        ArrayList<JSONObject> results = new ArrayList<>();

        JSONArray x = list.getJSONArray("@value");
        for (int i = 0; i < x.length();i++){
            results.add(x.getJSONObject(i));
        }
        return results;
    }

    /**
     * This method converts the initial HTTP response string into a Java ArrayList of JSONObjects.
     * @param response HTTP response string
     * @return ArrayList of JSON Objects
     */
    static public ArrayList<JSONObject> gremlinResponseToResultArrayList (String response) {

        JSONObject responseJson = new JSONObject(response);             // HTTP response is JSONObject
        JSONArray resultList = responseJson.getJSONObject("result")     // Result list is stored in "data" JSONObject
                .getJSONObject("data").getJSONArray("@value");

        ArrayList<JSONObject> results = new ArrayList<>();
        for (int i = 0; i < resultList.length(); i++) {
            results.add(resultList.getJSONObject(i));
        }

        return results;

    }

    /**
     * This method converts a Gremlin map into a Java HashMap of String-JSONObject pairs
     * @param gremlinMap JSONObject containing a JSONArray (map)
     * @return HashMap with String key and JSONObject values
     */
    static public HashMap<String,JSONObject> gremlinMapToHashMap (JSONObject gremlinMap) {

        HashMap<String, JSONObject> result = new HashMap<>();
        if (gremlinMap.getString("@type").equals("g:Map")) {
            JSONArray pairs = gremlinMap.getJSONArray("@value");
            for (int i = 0; i < pairs.length(); i++) {
                String key = pairs.getString(i);
                JSONObject value = pairs.getJSONObject(i + 1);
                i = i + 1;
                result.put(key, value);
            }
        }

        return result;
    }


    /**
     * This method extracts the value from a Gremlin key-value pair. This must be a list.
     * @param propertyValue JSONObject representing the value in a HashMap key-value pair
     * @return String representation of the value. Note, returns null if the JSONObject is not a list -
     * all property values are lists
     */
    static public String getPropertyValue(JSONObject propertyValue) {

        String value = null;                                                    // init return value
        if (propertyValue.getString("@type").equals("g:List")) {           // check if list
            JSONArray propertyList = propertyValue.getJSONArray("@value"); // get property values
            if (propertyList.length() == 1) {                                   // int, string or other
                try {
                    value = propertyList.getString(0);
                } catch (JSONException e) {
                    value = propertyList.getJSONObject(0).get("@value").toString();
                }
            } else {                                                            // set or list
                ArrayList<String> propertyValues = new ArrayList<>();
                for (int i = 0; i < propertyList.length(); i++) {
                    propertyValues.add(propertyList.getString(i));
                }
                value = propertyValues.toString();
            }
        }
        return value;
    }
}
