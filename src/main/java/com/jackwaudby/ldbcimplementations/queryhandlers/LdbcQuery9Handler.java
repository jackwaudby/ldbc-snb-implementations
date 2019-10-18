package com.jackwaudby.ldbcimplementations.queryhandlers;

import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery9;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery9Result;

import java.util.ArrayList;
import java.util.HashMap;

import static com.jackwaudby.ldbcimplementations.utils.HttpResponseToResultList.httpResponseToResultList;

public class LdbcQuery9Handler  implements OperationHandler<LdbcQuery9, JanusGraphDb.JanusGraphConnectionState> {

    @Override
    public void executeOperation(LdbcQuery9 operation, JanusGraphDb.JanusGraphConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {

        // TODO: Add transaction logic to query string
        // TODO: Add transaction retry logic to response

        long personId = operation.personId();
        long date = operation.maxDate().getTime();

        JanusGraphDb.JanusGraphClient client = dbConnectionState.getClient();   // janusgraph client

        String queryString = "{\"gremlin\": \"" +                               // gremlin query string
                "g.V().has('Person','id',"+personId+")." +
                    "repeat(both('knows').simplePath()).emit().times(2).hasLabel('Person').dedup().as('person')." +
                    "in('hasCreator').has('creationDate',lt(new Date("+date+")))." +
                    "order().by('creationDate',desc).by('id',asc).limit(20).as('message')." +
                    "select('person','message').by(valueMap('id','firstName','lastName'))." +
                    "by(valueMap('id','creationDate','content','imageFile'))" +
                "\"" +
                "}";
        String response = client.execute(queryString);                          // execute query
        ArrayList<HashMap<String, String>> result                               // parse result
                = httpResponseToResultList(response);
        ArrayList<LdbcQuery9Result> endResult                                   // init result list
                = new ArrayList<>();
        for (int i = 0; i < result.size(); i++) {                               // for each result
            String messageContent;                                              // set message content
            if (result.get(i).get("messageContent").equals("")) {               // imagefile
                messageContent = result.get(i).get("messageImageFile");
            } else {                                                            // content
                messageContent = result.get(i).get("messageContent");
            }
            LdbcQuery9Result res                                                // create result object
                    = new LdbcQuery9Result(
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

