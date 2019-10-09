package com.jackwaudby.ldbcimplementations.queryhandlers;

import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery3PersonFriends;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery3PersonFriendsResult;

import java.util.HashMap;

import static com.jackwaudby.ldbcimplementations.utils.HttpResponseToResultMap.httpResponseToResultMap;

public class LdbcShortQuery3PersonFriendsHandler implements OperationHandler<LdbcShortQuery3PersonFriends, JanusGraphDb.JanusGraphConnectionState> {

    @Override
    public void executeOperation(LdbcShortQuery3PersonFriends operation, JanusGraphDb.JanusGraphConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {

        // get query parameter from operation
        long personId = operation.personId();
        // get JanusGraph client
        JanusGraphDb.JanusGraphClient client = dbConnectionState.getClient();
        // gremlin query string
        String queryString = "{\"gremlin\": \"" +
                "try {" +
                "personIds=g.V().has('Person','id'," + personId + ").outE('knows').inV().values('id').toList();[];" +
                "creationDates=g.V().has('Person','id'," + personId + ").outE('knows').values('creationDate').toList();[];" +
                "result=['personIds':personIds];[];" +
                "result['creationDates']=creationDates;[];" +
                "graph.tx().commit();[];" +
                "} catch (Exception e) {" +
                "errorMessage=[e.toString()];[];" +
                "result=[query_error:errorMessage];[];" +
                "graph.tx().rollback();[];" +
                "};" +
                "result;\"" +
                "}";

        int TX_ATTEMPTS = 0;
        int TX_RETRIES = 5;
        LdbcShortQuery3PersonFriendsResult endResult = null;
        while (TX_ATTEMPTS < TX_RETRIES) {
            System.out.println("Attempt " + (TX_ATTEMPTS + 1));
            String response = client.execute(queryString);                                // get response as string
            HashMap<String, String> result = httpResponseToResultMap(response);      // convert to result map
            if (result.containsKey("query_error")) {
                TX_ATTEMPTS = TX_ATTEMPTS + 1;
                System.out.println("Query Error: " + result.get("query_error"));
            } else if (result.containsKey("http_error")) {
                TX_ATTEMPTS = TX_ATTEMPTS + 1;
                System.out.println("Gremlin Server Error: " + result.get("http_error"));
            } else {

                // sorted on server side
                // get result as hashamp
                // for each element of hashamp create reuslt

                // create result object
//                endResult = new LdbcShortQuery3PersonFriendsResult()

//                        result.get("firstName"),
//                        result.get("lastName"),
//                        Long.parseLong(result.get("birthday")),
//                        result.get("locationIP"),
//                        result.get("browserUsed"),
//                        Long.parseLong(result.get("cityId")),
//                        result.get("gender"),
//                        Long.parseLong(result.get("creationDate")));

                break;
            }
        }
//        resultReporter.report(0, endResult, operation);
    }
}

// figure out http response
// Map vs list
// test query
// g.V().has('Person','id',1099511628716).as('friend').V(v).addE('knows').property('creationDate',newdate).next()
