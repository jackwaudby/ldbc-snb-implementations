package com.jackwaudby.ldbcimplementations.queryhandlers;

import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcNoResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate4AddForum;

import java.util.HashMap;
import java.util.List;

import static com.jackwaudby.ldbcimplementations.utils.HttpResponseToResultMap.httpResponseToResultMap;

public class LdbcUpdate4AddForumHandler implements OperationHandler<LdbcUpdate4AddForum, JanusGraphDb.JanusGraphConnectionState> {

    @Override
    public void executeOperation(LdbcUpdate4AddForum operation, JanusGraphDb.JanusGraphConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {

        long forumId = operation.forumId();
        String forumTitle = operation.forumTitle();
        long forumCreationDate = operation.creationDate().getTime();
        long moderatorId = operation.moderatorPersonId();
        List<Long> tagIds = operation.tagIds();

        // get JanusGraph client
        JanusGraphDb.JanusGraphClient client = dbConnectionState.getClient();
        // gremlin query string
        String queryString = "{\"gremlin\": \"try {" +
                "p = g.addV('Forum')" +
                ".property('id'," + forumId + ")" +
                ".property('title','" + forumTitle + "')" +
                ".property('creationDate','" + forumCreationDate  + "').next();[];" +
                "g.V().has('Person', 'id',"+ moderatorId +").as('person').V(p).addE('hasModerator').to('person').next();[];" +
                "tagid=" + tagIds.toString() + ";[];"+
                "for (item in tagid) { " +
                "g.V().has('Tag', 'id', item).as('tag').V(p).addE('hasTag').to('tag').next();[];" +
                "};" +
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
            String response = client.execute(queryString);                              // get response as string
            HashMap<String, String> result = httpResponseToResultMap(response);         // convert to result map
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
        resultReporter.report(0, LdbcNoResult.INSTANCE, operation);          // pass result to driver


    }
}