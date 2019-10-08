package com.jackwaudby.ldbcimplementations.queryhandlers;

import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery1PersonProfileResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery5MessageCreator;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery5MessageCreatorResult;

import java.util.HashMap;

import static com.jackwaudby.ldbcimplementations.utils.HttpResponseToResultMap.httpResponseToResultMap;

/**
 * Given a Message, retrieve its author and their ID, firstName and lastName.
 */
public class LdbcShortQuery5MessageCreatorHandler implements OperationHandler<LdbcShortQuery5MessageCreator, JanusGraphDb.JanusGraphConnectionState> {

    @Override
    public void executeOperation(LdbcShortQuery5MessageCreator operation, JanusGraphDb.JanusGraphConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {

        long messageId = operation.messageId();                                     // query parameters
        JanusGraphDb.JanusGraphClient client = dbConnectionState.getClient();       // get JanusGraph client
        String queryString = "{\"gremlin\": \"" +                                   // gremlin query string
                "try {" +
                "post_exists = g.V().has('Post','id','" + messageId + "').hasNext();[];" +
                "if (post_exists) {" +
                "v=g.V().has('Post','id','" + messageId + "').out('hasCreator').valueMap('id','firstName','lastName').next();[];" +
                "} else {" +
                "v=g.V().has('Comment','id','" + messageId + "').out('hasCreator').valueMap('id','firstName','lastName').next();[];" +
                "};" +
                "graph.tx().commit();[];" +
                "} catch (Exception e) {" +
                "errorMessage =[e.toString()];[];" +
                "v=[query_error:errorMessage];[];" +
                "graph.tx().rollback();[];" +
                "};" +
                "v;\"" +
                "}";

        int TX_ATTEMPTS = 0;
        int TX_RETRIES = 5;
        LdbcShortQuery5MessageCreatorResult endResult = null;
        while (TX_ATTEMPTS < TX_RETRIES) {
            System.out.println("Attempt " + (TX_ATTEMPTS + 1));
            String response = client.execute(queryString);                           // get http response
            HashMap<String, String> result = httpResponseToResultMap(response);      // convert to result to map
            if (result.containsKey("query_error")) {
                TX_ATTEMPTS = TX_ATTEMPTS + 1;
                System.out.println("Query Error: " + result.get("query_error"));
            } else if (result.containsKey("http_error")) {
                TX_ATTEMPTS = TX_ATTEMPTS + 1;
                System.out.println("Gremlin Server Error: " + result.get("http_error"));
            } else {
                endResult = new LdbcShortQuery5MessageCreatorResult(                 // create result object
                        Long.parseLong(result.get("id")),                      // author ID
                        result.get("firstName"),                               // author first name
                        result.get("lastName")                                 // author last name
                );
                break;
            }
        }
        resultReporter.report(0, endResult, operation);

    }
}
