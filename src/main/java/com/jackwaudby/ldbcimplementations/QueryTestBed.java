package com.jackwaudby.ldbcimplementations;

import org.json.JSONObject;
import java.util.*;

import static com.jackwaudby.ldbcimplementations.utils.GremlinResponseParsers.*;

/**
 * Query test bed provides functionality for testing queries in a client-server model.
 */
public class QueryTestBed {
    public static void main(String[] args) {

        String queryString = "{\"gremlin\": \"" +
                " g.V().has('Person','id',4398046514041)." +
                " choose(" +
                " repeat(both().dedup()).until(has('Person','id',3)).limit(1).path().count(local).is(gt(0))," +
                " repeat(both().simplePath()).until(has('Person','id',3)).limit(1).path().count(local)." +
                " constant(-1)" +
                " )." +
                " fold()" +
                "\"" +
                "}";

        Map< String,String> hm = new HashMap<>();
        hm.put("url","http://localhost:8182");
        JanusGraphDb x = new JanusGraphDb();
        x.init(hm);
        String response = x.execute(queryString);

        ArrayList<JSONObject> resultList = gremlinResponseToResultArrayList(response);
        System.out.println(resultList);

        x.onClose();
    }
}
