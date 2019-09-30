package com.jackwaudby.ldbcimplementations;

import com.jackwaudby.ldbcimplementations.utils.ParseResultMap;
import com.ldbc.driver.DbException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Test {
    public static void main(String[] args)  {




        Map< String,String> hm = new HashMap<>();
        hm.put("url","http://localhost:8182");
        JanusGraphDb x = new JanusGraphDb();
        x.init(hm);

        String query1 = "{\"gremlin\": \"def v = g.V().has('Person','id',5497558141619).next();[];" +
                "def hm = g.V(v).valueMap('firstName','lastName','birthday','locationIP','browserUsed','gender','creationDate').next();[];" +
                "def v2 = g.V(v).outE('isLocatedIn').inV().valueMap('id').next();[];" +
                "def cityId = v2['id'];[];" +
                "hm.put('cityId',cityId);[];" +
                "hm.toString()\"}";


//        String parameter = "34359797215";
        String parameter = "42949783512";
        String query4 =  "{\"gremlin\": \"post_exists = g.V().has('Post','id'," + parameter + ").hasNext();[];if(post_exists){v=g.V().has('Post','id'," + parameter + ").valueMap('creationDate','content','imageFile').next();[]} else {v=g.V().has('Comment','id'," + parameter + ").valueMap('creationDate','content','imageFile').next();[]};v.toString()\"}";

        String queryResult = x.execute(query4);
        HashMap<String, String> resultMap = ParseResultMap.resultToMap(queryResult);
        System.out.println(resultMap);
        x.onClose();




    }
}
