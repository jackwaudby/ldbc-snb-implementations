package com.jackwaudby.ldbcimplementations.utils;

import java.io.*;

class LineCount {

    static int lineCount(File child) {
        Reader in;
        LineNumberReader lnr;
        int linenumber = 0;
        try {
            in = new FileReader(child);
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
