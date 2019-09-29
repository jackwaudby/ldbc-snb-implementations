package com.jackwaudby.ldbcimplementations.utils;

class ExtractLabels {

    // extracts labels from file names
    static String[] extractLabels(String stringToSplit, String path) {

        // preprocess file, remove directory path and postfix
        stringToSplit = stringToSplit.replace(path, "");
        stringToSplit = stringToSplit.replace("_0_0.csv", "");

        // split file name to get vertex and edge labels
        String delimiter = "_";

        return stringToSplit.split(delimiter);

    }
}
