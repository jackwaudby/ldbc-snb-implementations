package com.jackwaudby.ldbcimplementations.queryhandlers;

import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import com.jackwaudby.ldbcimplementations.utils.HttpResponseToResultMap;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery4MessageContent;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery4MessageContentResult;
import org.json.JSONObject;

import java.text.ParseException;
import java.util.HashMap;

import static com.jackwaudby.ldbcimplementations.utils.HttpResponseToResultMap.httpResponseToResultMap;

/**
 * Given a Message, retrieve its content and creation date.
 */
public class LdbcShortQuery4MessageContentHandler implements OperationHandler<LdbcShortQuery4MessageContent,JanusGraphDb.JanusGraphConnectionState> {

    //TODO: Add transaction and retry logic

    @Override
    public void executeOperation(LdbcShortQuery4MessageContent operation, JanusGraphDb.JanusGraphConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {

        long messageId = operation.messageId();

        // get JanusGraph client
        JanusGraphDb.JanusGraphClient client = dbConnectionState.getClient();
        // gremlin query string
        String queryString = "{\"gremlin\": \"" +
                "try {" +
                "v = g.V().has('Comment','id', " + messageId + ").fold()" +
                ".coalesce(unfold(),V().has('Post','id'," + messageId + "))" +
                ".valueMap('creationDate','content','imageFile');[];" +
                "graph.tx().commit();[];" +
                "} catch (Exception e) {" +
                "errorMessage =[e.toString()];[];" +
                "v=[query_error:errorMessage];[];" +
                "graph.tx().rollback();[];" +
                "};" +
                "v;" +
                "\"" +
                "}";


        int TX_ATTEMPTS = 0;
        int TX_RETRIES = 5;
        LdbcShortQuery4MessageContentResult endResult = null;
        while (TX_ATTEMPTS < TX_RETRIES) {
            System.out.println("Attempt " + (TX_ATTEMPTS + 1));
            String response = client.execute(queryString);                                // get response as string
            HashMap<String, String> result = httpResponseToResultMap(response);      // convert to result map
            if (result.containsKey("query_error")) {
                TX_ATTEMPTS = TX_ATTEMPTS + 1;
                System.out.println("Query Error: " + result.get("query_error"));
                System.out.println("Message ID: " + messageId);
            } else if (result.containsKey("http_error")) {
                TX_ATTEMPTS = TX_ATTEMPTS + 1;
                System.out.println("Gremlin Server Error: " + result.get("http_error"));
            } else {
                if (result.get("content").equals("")) {
                    endResult = new LdbcShortQuery4MessageContentResult(
                            result.get("imageFile"),
                            Long.parseLong(result.get("creationDate")));
                } else {
                    endResult = new LdbcShortQuery4MessageContentResult(
                            result.get("content"),
                            Long.parseLong(result.get("creationDate")));
                }
                break;
            }
        }
        resultReporter.report(0, endResult, operation);
    }
}
