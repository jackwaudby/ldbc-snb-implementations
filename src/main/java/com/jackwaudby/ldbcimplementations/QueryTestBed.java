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
                "g.withSack(0).V().has('Person','id',3298534884077)." +
                "repeat(both('knows').simplePath().sack(sum).by(constant(1))).emit().times(3)." +
                "dedup().has('Person','firstName','Karim')." +
                "order().by(sack(),asc).by('lastName',asc).by('id',asc)." +
                "local(union(" +
                "valueMap('lastName','id','email','birthday','creationDate','gender','browserUsed','locationIP','speaks'), " +
                "out('isLocatedIn').valueMap('name')," +
                "sack().fold(), " +
                "outE('workAt').as('workFrom').inV().as('company').out('isLocatedIn').as('country')." +
                "select('company','workFrom','country').by(valueMap('name')).by(valueMap()).fold()," +
                "outE('studyAt').as('studyFrom').inV().as('university').out('isLocatedIn').as('country')." +
                "select('university','studyFrom','country').by(valueMap('name')).by(valueMap()).fold()).fold())" +
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
