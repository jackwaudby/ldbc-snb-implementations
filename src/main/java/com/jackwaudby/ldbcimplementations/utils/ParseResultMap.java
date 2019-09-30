package com.jackwaudby.ldbcimplementations.utils;

import java.util.HashMap;

/**
 * This script provides a method that converts a result string to a map.
 */
public class ParseResultMap {

    public static HashMap<String,String> resultToMap(String result) {

        // remove outer brackets
        String resultString = result.replaceAll("],", "]|");
        resultString = resultString.replaceAll("[\\[\\]]", "");
        System.out.println(resultString);
        // split in key/value pairs
        String[] keyValuePairs = resultString.split("\\|");
        System.out.println(keyValuePairs.length);
        HashMap<String, String> resultMap = new HashMap<>();
        for (String pair : keyValuePairs)                        //iterate over the pairs
        {
            String[] entry = pair.split(":", 2);    //split the pairs to get key and value
            resultMap.put(entry[0].trim(), entry[1].trim());    //add to map and trim whitespaces
        }
        return resultMap;
    }
}
