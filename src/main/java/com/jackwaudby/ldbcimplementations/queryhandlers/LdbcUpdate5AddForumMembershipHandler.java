package com.jackwaudby.ldbcimplementations.queryhandlers;

import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcNoResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate5AddForumMembership;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate8AddFriendship;

import java.util.HashMap;

import static com.jackwaudby.ldbcimplementations.utils.HttpResponseToResultMap.httpResponseToResultMap;

public class LdbcUpdate5AddForumMembershipHandler implements OperationHandler<LdbcUpdate5AddForumMembership, JanusGraphDb.JanusGraphConnectionState> {

    @Override
    public void executeOperation(LdbcUpdate5AddForumMembership operation, JanusGraphDb.JanusGraphConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {

        long personId = operation.personId();
        long forumId = operation.forumId();
        long joinDate = operation.joinDate().getTime();

        // get JanusGraph client
        JanusGraphDb.JanusGraphClient client = dbConnectionState.getClient();

        // gremlin query string
        String queryString = "{\"gremlin\": \"try {" +
                "v = g.V().has('Person','id'," +
                personId +
                ").next();[];" +
                "g.V().has('Forum', 'id'," +
                forumId +
                ").as('member').V(v).addE('hasMember').property('joinDate'," +
                joinDate +
                ").to('member').next();[];" +
                "graph.tx().commit();[];" +
                "queryOutcome=['success'];[];" +
                "hm=[query_outcome:queryOutcome];[];" +
                "} catch (Exception e) {" +
                "errorMessage =[e.toString()];[];" +
                "hm=[query_error:errorMessage];[];" +
                "graph.tx().rollback();[];" +
                "};" +
                "hm;\"" +
                "}";

        int TX_ATTEMPTS = 0;
        int TX_RETRIES = 5;
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
                System.out.println(result.get("query_outcome"));
                break;
            }
        }
        // pass result to driver
        resultReporter.report(0, LdbcNoResult.INSTANCE, operation);

    }
}