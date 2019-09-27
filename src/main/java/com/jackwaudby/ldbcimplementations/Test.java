package com.jackwaudby.ldbcimplementations;

import com.ldbc.driver.DbException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Test {
    public static void main(String[] args) throws IOException {

        //java -cp "target/janusgraph-1.0-SNAPSHOT.jar:target/dependencies/*" com.ldbc.driver.Client -P validation/interactive-validate.properties

        Map< String,String> hm = new HashMap<>();
        hm.put("url","http://localhost:8182");
        JanusGraphDb x = new JanusGraphDb();
        x.init(hm);
        String y = x.execute("{\"gremlin\": \"def v = g.V().has('Person','id',5497558141619).next();[];def hm = g.V(v).valueMap('firstName','lastName','birthday','locationIP','browserUsed','gender','creationDate').next();[];def v2 = g.V(v).outE('isLocatedIn').inV().valueMap('id').next();[]; def cityId = v2['id'];[];hm.put('cityId',cityId);[];hm.toString()\"}");
        System.out.println(y);
        x.onClose();

    }
}
