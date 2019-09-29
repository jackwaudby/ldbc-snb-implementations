package com.jackwaudby.ldbcimplementations.utils;

class TagClassFix {

    // tag class fix
    static String tagClassFix(String s) {
        if (s.contentEquals("Tagclass")) {
            s = s.substring(0, 3) + s.substring(3, 4).toUpperCase() + s.substring(4);
        }
        return s;
    }
}
