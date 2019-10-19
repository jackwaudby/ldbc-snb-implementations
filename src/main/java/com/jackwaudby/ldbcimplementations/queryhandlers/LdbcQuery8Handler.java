package com.jackwaudby.ldbcimplementations.queryhandlers;

import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery8;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery8Result;

import java.util.ArrayList;
import java.util.HashMap;

import static com.jackwaudby.ldbcimplementations.utils.HttpResponseToResultList.httpResponseToResultList;

public class LdbcQuery8Handler implements OperationHandler<LdbcQuery8, JanusGraphDb.JanusGraphConnectionState> {

    @Override
    public void executeOperation(LdbcQuery8 operation, JanusGraphDb.JanusGraphConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {

        // TODO: Add transaction logic to query string
        // TODO: Add transaction retry logic to response

        long personId = operation.personId();
        int limit = operation.limit();

        JanusGraphDb.JanusGraphClient client = dbConnectionState.getClient();   // janusgraph client

        String queryString = "{\"gremlin\": \"" +                               // gremlin query string
                "g.V().has('Person','id',"+personId+")." +
                "in('hasCreator')." +
                "in('replyOf').as('message')." +
                "order().by('creationDate',desc).by('id',asc).limit("+limit+")." +
                "out('hasCreator').as('person')." +
                "select('message','person')." +
                "by(valueMap('id','creationDate','content'))." +
                "by(valueMap('id','firstName','lastName'))" +

                "\"" +
                "}";
        String response = client.execute(queryString);                          // execute query
        ArrayList<HashMap<String, String>> result                               // parse result
                = httpResponseToResultList(response);
        ArrayList<LdbcQuery8Result> endResult                                   // init result list
                = new ArrayList<>();
        for (int i = 0; i < result.size(); i++) {                               // for each result
            LdbcQuery8Result res                                                // create result object
                    = new LdbcQuery8Result(
                    Long.parseLong(result.get(i).get("personId")),              // personId
                    result.get(i).get("personFirstName"),                       // personFirstName
                    result.get(i).get("personLastName"),                        // personLastName
                    Long.parseLong(result.get(i).get("messageCreationDate")),   // messageCreationDate
                    Long.parseLong(result.get(i).get("messageId")),             // messageId
                    result.get(i).get("messageContent")                         // messageContent
            );
            endResult.add(res);                                                 // add to result list
        }
        resultReporter.report(0, endResult, operation);
    }
}