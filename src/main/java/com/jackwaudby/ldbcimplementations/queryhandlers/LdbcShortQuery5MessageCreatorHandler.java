package com.jackwaudby.ldbcimplementations.queryhandlers;

import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery5MessageCreator;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery5MessageCreatorResult;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import java.util.ArrayList;

import static com.jackwaudby.ldbcimplementations.utils.GremlinResponseParsers.*;
import static com.jackwaudby.ldbcimplementations.utils.ImplementationConfiguration.getTxnAttempts;

/**
 * Given a Message, retrieve its author and their ID, firstName and lastName.
 */
public class LdbcShortQuery5MessageCreatorHandler implements OperationHandler<LdbcShortQuery5MessageCreator, JanusGraphDb.JanusGraphConnectionState> {

    private static Logger LOGGER = Logger.getLogger(LdbcShortQuery5MessageCreatorHandler.class.getName());

    @Override
    public void executeOperation(LdbcShortQuery5MessageCreator operation, JanusGraphDb.JanusGraphConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {

        long messageId = operation.messageId();                                     // message id
        JanusGraphDb.JanusGraphClient client = dbConnectionState.getClient();       // get JanusGraph client
        String queryString = "{\"gremlin\": \"" +
                "graph.tx().rollback();[];" +
                "try{" +
                "result=g.V().has('Comment','id',"+messageId+").fold()" +
                ".coalesce(unfold(),V().has('Post','id',"+messageId+"))" +
                ".out('hasCreator').valueMap('id','firstName','lastName').toList();[];" +
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
            LOGGER.info("Attempt " + (TX_ATTEMPTS + 1) + ": " + LdbcShortQuery5MessageCreatorHandler.class.getSimpleName());
            String response = client.execute(queryString);                                            // execute query
            ArrayList<JSONObject> results = gremlinResponseToResultArrayList(response);          // get result list
            if (gremlinMapToHashMap(results.get(0)).containsKey("error")) {
                LOGGER.error(getPropertyValue(gremlinMapToHashMap(results.get(0)).get("error")));
                TX_ATTEMPTS = TX_ATTEMPTS + 1;
            } else {

                long personId = Long.parseLong(getPropertyValue(gremlinMapToHashMap(results.get(0)).get("id")));
                String firstName = getPropertyValue(gremlinMapToHashMap(results.get(0)).get("firstName"));
                String lastName = getPropertyValue(gremlinMapToHashMap(results.get(0)).get("lastName"));

                LdbcShortQuery5MessageCreatorResult queryResult = new LdbcShortQuery5MessageCreatorResult(                 // create result object
                        personId,
                        firstName,
                        lastName
                );
                resultReporter.report(0, queryResult, operation);

                break;
            }
        }



    }
}
