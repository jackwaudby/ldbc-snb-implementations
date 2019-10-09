package com.jackwaudby.ldbcimplementations.queryhandlers;

import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery1PersonProfile;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery1PersonProfileResult;

import java.util.ArrayList;
import java.util.HashMap;

import static com.jackwaudby.ldbcimplementations.utils.HttpResponseToResultList.httpResponseToResultList;
import static com.jackwaudby.ldbcimplementations.utils.HttpResponseToResultMap.httpResponseToResultMap;

/**
 * Given a start Person, retrieve their first name, last name, birthday, IP
 * address, browser, and city of residence.
 */
public class LdbcShortQuery1PersonProfileHandler implements OperationHandler<LdbcShortQuery1PersonProfile, JanusGraphDb.JanusGraphConnectionState> {

    // TODO: Add transaction logic to query string
    // TODO: Add transaction retry logic to response

    @Override
    public void executeOperation(LdbcShortQuery1PersonProfile operation, JanusGraphDb.JanusGraphConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {

        // get query parameter from operation
        long personId = operation.personId();
        // get JanusGraph client
        JanusGraphDb.JanusGraphClient client = dbConnectionState.getClient();
        // gremlin query string
        String queryString = "{\"gremlin\": \"" +                               // gremlin query string
                "g.V().has('Person','id'," + personId + ").as('person').out('isLocatedIn').as('city').select('person','city').by(valueMap('firstName','lastName','birthday','locationIP','browserUsed','gender','creationDate')).by(valueMap('id')).toList();" +
                "\"" +
                "}";

        String response = client.execute(queryString);                          // execute query
        ArrayList<HashMap<String, String>> result                               // parse result
                = httpResponseToResultList(response);
        ArrayList<LdbcShortQuery1PersonProfileResult> endResult                   // init result list
                = new ArrayList<>();
        for (int i = 0; i < result.size(); i++) {                               // for each result
            LdbcShortQuery1PersonProfileResult res                                    // create result object
                    = new LdbcShortQuery1PersonProfileResult(result.get(i).get("firstName"),
                        result.get(i).get("lastName"),
                        Long.parseLong(result.get(i).get("birthday")),
                        result.get(i).get("locationIP"),
                        result.get(i).get("browserUsed"),
                        Long.parseLong(result.get(i).get("cityId")),
                        result.get(i).get("gender"),
                        Long.parseLong(result.get(i).get("creationDate"))
            );
            endResult.add(res);                                                 // add to result list
        }

//        String queryString = "{\"gremlin\": \"" +
//                "try {" +
//                "v = g.V().has('Person','id'," + personId + ").next();[];" +
//                "hm = g.V(v).valueMap('firstName','lastName','birthday','locationIP','browserUsed','gender','creationDate').next();[];" +
//                "v2 = g.V(v).outE('isLocatedIn').inV().valueMap('id').next();[];" +
//                "cityId = v2['id'];[];" +
//                "hm.put('cityId',cityId);[];" +
//                "graph.tx().commit();[];" +
//                "} catch (Exception e) {" +
//                "errorMessage =[e.toString()];[];" +
//                "hm=[query_error:errorMessage];[];" +
//                "graph.tx().rollback();[];" +
//                "};" +
//                "hm;\"" +
//                "}";


//        int TX_ATTEMPTS = 0;
//        int TX_RETRIES = 5;
//        LdbcShortQuery1PersonProfileResult endResult = null;
//        while (TX_ATTEMPTS < TX_RETRIES) {
//            System.out.println("Attempt " + (TX_ATTEMPTS + 1));
//            String response = client.execute(queryString);                                // get response as string
//            HashMap<String, String> result = httpResponseToResultMap(response);      // convert to result map
//            if (result.containsKey("query_error")) {
//                TX_ATTEMPTS = TX_ATTEMPTS + 1;
//                System.out.println("Query Error: " + result.get("query_error"));
//            } else if (result.containsKey("http_error")) {
//                TX_ATTEMPTS = TX_ATTEMPTS + 1;
//                System.out.println("Gremlin Server Error: " + result.get("http_error"));
//            } else {
//                // create result object
//                endResult = new LdbcShortQuery1PersonProfileResult(
//                        result.get("firstName"),
//                        result.get("lastName"),
//                        Long.parseLong(result.get("birthday")),
//                        result.get("locationIP"),
//                        result.get("browserUsed"),
//                        Long.parseLong(result.get("cityId")),
//                        result.get("gender"),
//                        Long.parseLong(result.get("creationDate")));
//
//                break;
//            }
//        }
        resultReporter.report(0, endResult.get(0), operation);
    }
}
