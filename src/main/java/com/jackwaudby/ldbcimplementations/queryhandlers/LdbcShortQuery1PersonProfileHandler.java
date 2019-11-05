package com.jackwaudby.ldbcimplementations.queryhandlers;

import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import static com.jackwaudby.ldbcimplementations.utils.ImplementationConfiguration.getTxnAttempts;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery1PersonProfile;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery1PersonProfileResult;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import java.util.ArrayList;

import static com.jackwaudby.ldbcimplementations.utils.GremlinResponseParsers.*;
import static com.jackwaudby.ldbcimplementations.utils.GremlinResponseParsers.gremlinMapToHashMap;

/**
 * Given a start Person, retrieve their first name, last name, birthday, IP
 * address, browser, gender, creation date and the ID of their city of residence.
 */
public class LdbcShortQuery1PersonProfileHandler implements OperationHandler<LdbcShortQuery1PersonProfile, JanusGraphDb.JanusGraphConnectionState> {

    private static Logger LOGGER = Logger.getLogger(LdbcShortQuery1PersonProfileHandler.class.getName());

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

        int TX_ATTEMPTS = 0;                                                                // init. transaction attempts
        int TX_RETRIES = getTxnAttempts();                                                  // get max attempts
        while (TX_ATTEMPTS < TX_RETRIES) {
            LOGGER.info("Attempt " + (TX_ATTEMPTS + 1) + ": " +
                    LdbcShortQuery1PersonProfileHandler.class.getSimpleName());
            String response = client.execute(queryString);                                       // execute query
            ArrayList<JSONObject> results = gremlinResponseToResultArrayList(response);          // get result list
            if (gremlinMapToHashMap(results.get(0)).containsKey("error")) {                         // check if failed
                LOGGER.error(getPropertyValue(gremlinMapToHashMap(results.get(0)).get("error")));
                TX_ATTEMPTS = TX_ATTEMPTS + 1;
            } else {
                ArrayList<JSONObject> result = gremlinListToArrayList(results.get(0));               // get result

                try {
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
                    break;
                } catch (Exception e) {
                    LOGGER.error(e);
                    TX_ATTEMPTS = TX_ATTEMPTS + 1;
                }
            }
        }
    }
}

