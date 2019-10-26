package com.jackwaudby.ldbcimplementations;

import com.sun.xml.internal.ws.protocol.soap.ClientMUTube;
import org.json.JSONObject;
import java.util.*;

import static com.jackwaudby.ldbcimplementations.utils.GremlinResponseParsers.*;

/**
 * Query test bed provides functionality for testing queries in a client-server model.
 */
public class QueryTestBed {
    public static void main(String[] args) {

        // ["com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery3",5497558139286,"Lithuania","Estonia",1283299200000,41,20]|
// [[2194,"Alexander","Dobrunov",2,1,3],
// [1490,"John","Wilson",1,1,2],
// [1099511628352,"Joseph","Linchuk",1,1,2],
// [3298534883809,"Antonio","Alvarez",1,1,2]]

        long personId = 5497558139286L;
        String countryX = "Lithuania";
        String countryY = "Estonia";
        long startDate = 1283299200000L;
        long duration = 41L;
        long endDate = startDate + (duration * 86400000L);
        long limit = 20;


        String queryString = "{\"gremlin\": \"" +
                "g.V().has('Person','id',"+personId+").repeat(both('knows').simplePath()).emit().times(2).dedup()." +
                "where(and(out('isLocatedIn').out('isPartOf').has('name',without('"+countryX+"','"+countryY+"'))," +
                "local(__.in('hasCreator').has('creationDate',between(new Date("+startDate+"),new Date("+endDate+")))." +
                "out('isLocatedIn').has('name','"+countryX+"').count().is(gt(0)))," +
                "local(__.in('hasCreator').has('creationDate',between(new Date("+startDate+"),new Date("+endDate+")))." +
                "out('isLocatedIn').has('name','"+countryY+"').count().is(gt(0)))))." +
                "order().by(__.in('hasCreator').has('creationDate',between(new Date("+startDate+"),new Date("+endDate+")))." +
                "out('isLocatedIn').has('name','"+countryX+"').count(),desc).by('id',asc)." +
                "local(union(identity().valueMap('id','firstName','lastName').by(unfold()).unfold(),__.in('hasCreator')." +
                "has('creationDate',between(new Date("+startDate+"),new Date("+endDate+")))."+
                "out('isLocatedIn').has('name',within('"+countryX+"','"+countryY+"')).group().by('name').by(count()).unfold()).fold())" +
                "\"" +
                "}";

        Map< String,String> hm = new HashMap<>();
        hm.put("url","http://localhost:8182");
        JanusGraphDb x = new JanusGraphDb();
        x.init(hm);
        String response = x.execute(queryString);
        ArrayList<JSONObject> results = gremlinResponseToResultArrayList(response);
        System.out.println(results);

        x.onClose();
    }
}
