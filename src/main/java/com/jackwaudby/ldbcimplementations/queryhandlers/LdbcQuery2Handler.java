package com.jackwaudby.ldbcimplementations.queryhandlers;

import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery2;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery2Result;

import java.util.ArrayList;
import java.util.HashMap;

import static com.jackwaudby.ldbcimplementations.utils.HttpResponseToResultList.httpResponseToResultList;

public class LdbcQuery2Handler implements OperationHandler<LdbcQuery2, JanusGraphDb.JanusGraphConnectionState> {

    @Override
    public void executeOperation(LdbcQuery2 operation, JanusGraphDb.JanusGraphConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {

        // TODO: Add transaction logic to query string
        // TODO: Add transaction retry logic to response

        long personId = operation.personId();
        long date = operation.maxDate().getTime();
        int limit = operation.limit();

        JanusGraphDb.JanusGraphClient client = dbConnectionState.getClient();   // janusgraph client

        String queryString = "{\"gremlin\": \"" +                               // gremlin query string
                "g.V().has('Person','id',"+personId+").both('knows').as('person')." +
                "in('hasCreator').has('creationDate',lte(new Date("+date+")))." +
                "order().by('creationDate',desc).by('id',asc).limit("+limit+").as('message')." +
                "select('person','message')." +
                "by(valueMap('id','firstName','lastName'))." +
                "by(valueMap('id','imageFile','content','creationDate'))" +
                "\"" +
                "}";
        String response = client.execute(queryString);                          // execute query
        ArrayList<HashMap<String, String>> result                               // parse result
                = httpResponseToResultList(response);
        ArrayList<LdbcQuery2Result> endResult                                   // init result list
                = new ArrayList<>();
        for (int i = 0; i < result.size(); i++) {                               // for each result
            String messageContent;                                              // set message content
            if (result.get(i).get("messageContent").equals("")) {               // imagefile
                messageContent = result.get(i).get("messageImageFile");
            } else {                                                            // content
                messageContent = result.get(i).get("messageContent");
            }
            LdbcQuery2Result res                                                // create result object
                    = new LdbcQuery2Result(
                    Long.parseLong(result.get(i).get("personId")),              // personId
                    result.get(i).get("personFirstName"),                       // personFirstName
                    result.get(i).get("personLastName"),                        // personLastName
                    Long.parseLong(result.get(i).get("messageId")),             // messageId
                    messageContent, // messageContent
                    Long.parseLong(result.get(i).get("messageCreationDate"))    // messageCreationDate
            );
            endResult.add(res);                                                 // add to result list
        }
        resultReporter.report(0, endResult, operation);
    }
}

