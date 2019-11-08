package com.jackwaudby.ldbcimplementations.queryhandlers;

import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery4MessageContent;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery4MessageContentResult;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import java.util.ArrayList;

import static com.jackwaudby.ldbcimplementations.utils.GremlinResponseParsers.*;
import static com.jackwaudby.ldbcimplementations.utils.ImplementationConfiguration.getTxnAttempts;

/**
 * Given a Message ID, retrieve its content or imagefile and creation date.
 */
public class LdbcShortQuery4MessageContentHandler implements OperationHandler<LdbcShortQuery4MessageContent,JanusGraphDb.JanusGraphConnectionState> {

    private static Logger LOGGER = Logger.getLogger(LdbcShortQuery4MessageContentHandler.class.getName());


    @Override
    public void executeOperation(LdbcShortQuery4MessageContent operation, JanusGraphDb.JanusGraphConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {

        long messageId = operation.messageId();

        // get JanusGraph client
        JanusGraphDb.JanusGraphClient client = dbConnectionState.getClient();
        // gremlin query string
        String queryString = "{\"gremlin\": \"" +
                "graph.tx().rollback();[];" +
                "try{" +
                "result = g.V().has('Comment','id', "+messageId+").fold()" +
                ".coalesce(unfold(),V().has('Post','id',"+messageId+"))" +
                ".valueMap('creationDate','content','imageFile').toList();[];" +
                "graph.tx().commit();[];"+
                "} catch (Exception e) {"+
                "errorMessage =[e.toString()];[];" +
                "result=[error:errorMessage];" +
                "graph.tx().rollback();[];" +
                "};" +
                "result" +
                "\"" +
                "}";


        int TX_ATTEMPTS = 0;
        int TX_RETRIES = getTxnAttempts();

        while (TX_ATTEMPTS < TX_RETRIES) {
            LOGGER.info("Attempt " + (TX_ATTEMPTS + 1) + ": " + LdbcShortQuery4MessageContentHandler.class.getSimpleName());
            String response = client.execute(queryString);                                          // execute query
            ArrayList<JSONObject> results = gremlinResponseToResultArrayList(response);             // get result list
            if (gremlinMapToHashMap(results.get(0)).containsKey("error")) {

                LOGGER.error(getPropertyValue(gremlinMapToHashMap(results.get(0)).get("error")));
                TX_ATTEMPTS = TX_ATTEMPTS + 1;

            } else {

                long creationDate =
                        Long.parseLong(getPropertyValue(gremlinMapToHashMap(results.get(0)).get("creationDate")));
                String messageContent;
                if (gremlinMapToHashMap(results.get(0)).containsKey("imageFile") &&
                        !getPropertyValue(gremlinMapToHashMap(results.get(0)).get("imageFile")).equals("")) {

                    messageContent = getPropertyValue(gremlinMapToHashMap(results.get(0)).get("imageFile"));

                } else {

                    messageContent = getPropertyValue(gremlinMapToHashMap(results.get(0)).get("content"));

                }

                LdbcShortQuery4MessageContentResult queryResult = new LdbcShortQuery4MessageContentResult(
                        messageContent,
                        creationDate
                );

                resultReporter.report(0, queryResult,operation);
                break;
            }
        }

    }
}
