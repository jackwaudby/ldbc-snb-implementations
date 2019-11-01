package com.jackwaudby.ldbcimplementations.queryhandlers;

import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import com.jackwaudby.ldbcimplementations.utils.ImplementationConfiguration;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery1PersonProfile;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery1PersonProfileResult;
import org.json.JSONObject;

import java.util.ArrayList;

import static com.jackwaudby.ldbcimplementations.utils.GremlinResponseParsers.*;
import static com.jackwaudby.ldbcimplementations.utils.GremlinResponseParsers.gremlinMapToHashMap;

/**
 * Given a start Person, retrieve their first name, last name, birthday, IP
 * address, browser, gender, creation date and the ID of their city of residence.
 */
public class LdbcShortQuery1PersonProfileHandler implements OperationHandler<LdbcShortQuery1PersonProfile, JanusGraphDb.JanusGraphConnectionState> {

    // TODO: Add transaction retry logic to response

    @Override
    public void executeOperation(LdbcShortQuery1PersonProfile operation, JanusGraphDb.JanusGraphConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {

        long personId = operation.personId();                                   // get query parameter from operation
        JanusGraphDb.JanusGraphClient client = dbConnectionState.getClient();   // get JanusGraph client
        String queryString = "{\"gremlin\": \"" +                               // gremlin query string
                "graph.tx().rollback();[];" +
                "try{" +
                  "result = g.V().has('Person','id',"+personId+")." +
                             "union(" +
                               "valueMap('firstName','lastName','birthday','locationIP','browserUsed','gender','creationDate').unfold(),"+
                               "out('isLocatedIn').valueMap('id').unfold()" +
                             ").fold().toList();" +
                  "graph.tx().commit();[];"+
                "} catch (Exception e) {"+
                  "errorMessage =[e.toString()];[];" +
                  "result=[error:errorMessage];" +
                  "graph.tx().rollback();[];" +
                "};" +
                "result" +
                "\"" +
                "}";

        int TX_ATTEMPTS = 0;                                                                 // init. transaction attempts
        int TX_RETRIES = new ImplementationConfiguration().getTxnAttempts();
        String response = client.execute(queryString);                                       // execute query
        ArrayList<JSONObject> results = gremlinResponseToResultArrayList(response);          // get result list
        ArrayList<JSONObject> result = gremlinListToArrayList(results.get(0));               // get result
        LdbcShortQuery1PersonProfileResult ldbcShortQuery1PersonProfileResult                // create result object
                = new LdbcShortQuery1PersonProfileResult(
                getPropertyValue(gremlinMapToHashMap(result.get(3)).get("firstName")),
                getPropertyValue(gremlinMapToHashMap(result.get(4)).get("lastName")),
                Long.parseLong(getPropertyValue(gremlinMapToHashMap(result.get(6)).get("birthday"))),
                getPropertyValue(gremlinMapToHashMap(result.get(2)).get("locationIP")),
                getPropertyValue(gremlinMapToHashMap(result.get(1)).get("browserUsed")),
                Long.parseLong(getPropertyValue(gremlinMapToHashMap(result.get(7)).get("id"))),
                getPropertyValue(gremlinMapToHashMap(result.get(5)).get("gender")),
                Long.parseLong(getPropertyValue(gremlinMapToHashMap(result.get(0)).get("creationDate")))
        );

        resultReporter.report(0, ldbcShortQuery1PersonProfileResult, operation); // pass to driver

    }
}









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

