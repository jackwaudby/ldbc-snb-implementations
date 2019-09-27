package com.jackwaudby.ldbcimplementations.utils;

import java.util.HashMap;

public class ParseResultMap {

    public static HashMap<String,String> resultToMap(String result) {

        // remove outer brackets
        String resultString = result.replaceAll("[\\[\\]]", "");
        // split in key/value pairs
        String[] keyValuePairs = resultString.split(",");

        HashMap<String, String> resultMap = new HashMap<>();
        for (String pair : keyValuePairs)                        //iterate over the pairs
        {
            String[] entry = pair.split(":", 2);    //split the pairs to get key and value
            resultMap.put(entry[0].trim(), entry[1].trim());    //add to map and trim whitespaces
        }
        return resultMap;
    }
}
