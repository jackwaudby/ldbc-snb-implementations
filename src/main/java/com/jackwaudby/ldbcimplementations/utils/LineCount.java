package com.jackwaudby.ldbcimplementations.utils;

import java.io.*;

/**
 * This script provides a method that counts the number of lines in a file.
 */
class LineCount {

    static int lineCount(File file) {
        Reader in;
        LineNumberReader lnr;
        int linenumber = 0;
        try {
            in = new FileReader(file);
            lnr = new LineNumberReader(in);
            while (lnr.readLine() != null) {
                linenumber++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return (linenumber-1);
    }
}
