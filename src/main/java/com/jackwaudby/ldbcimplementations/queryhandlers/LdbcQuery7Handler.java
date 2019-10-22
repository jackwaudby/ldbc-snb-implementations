package com.jackwaudby.ldbcimplementations.queryhandlers;

import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery7;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery7Result;

import java.util.ArrayList;
import java.util.HashMap;

import static com.jackwaudby.ldbcimplementations.utils.HttpResponseToResultList.httpResponseToResultList;

public class LdbcQuery7Handler implements OperationHandler<LdbcQuery7, JanusGraphDb.JanusGraphConnectionState> {

    @Override
    public void executeOperation(LdbcQuery7 operation, JanusGraphDb.JanusGraphConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {

        // TODO: Add transaction logic to query string
        // TODO: Add transaction retry logic to response

        long personId = operation.personId();
        long limit = operation.limit();

        JanusGraphDb.JanusGraphClient client = dbConnectionState.getClient();   // janusgraph client

        String queryString = "{\"gremlin\": \"" +                               // gremlin query string
                "g.V().has('Person','id'," + personId + ").in('hasCreator').as('message')." +
                "order().by('creationDate',desc).by('id',asc)" +
                "inE('likes').as('like')." +
                "order().by('creationDate',desc).outV().as('person').dedup().limit("+limit+")" +
                "choose(both('knows').has('Person','id'," + personId + "),constant(false),constant(true)).as('isNew')." +
                "select('person','message','isNew','like')." +
                "by(valueMap('id','firstName','lastName'))." +
                "by(valueMap('id','content','imageFile','creationDate'))." +
                "by(fold())." +
                "by(valueMap('creationDate'))" +
                "\"" +
                "}";
        String response = client.execute(queryString);                          // execute query
        ArrayList<HashMap<String, String>> result                               // parse result
                = httpResponseToResultList(response);
        ArrayList<LdbcQuery7Result> endResult                                   // init result list
                = new ArrayList<>();
        for (int i = 0; i < result.size(); i++) {                               // for each result
            String messageContent;                                              // set message content
            if (result.get(i).get("messageContent").equals("")) {               // imagefile
                messageContent = result.get(i).get("messageImageFile");
            } else {                                                            // content
                messageContent = result.get(i).get("messageContent");
            }
            long minutesLatency =
                    (Long.parseLong(result.get(i).get("likeCreationDate")) -
                    Long.parseLong(result.get(i).get("messageCreationDate"))) / 60000;
            Integer latency = (int) (long) minutesLatency;
            boolean isNew = Boolean.parseBoolean(result.get(i).get("isNew"));
            LdbcQuery7Result res                                                // create result object
                    = new LdbcQuery7Result(
                    Long.parseLong(result.get(i).get("personId")),              // personId
                    result.get(i).get("personFirstName"),                       // personFirstName
                    result.get(i).get("personLastName"),                        // personLastName
                    Long.parseLong(result.get(i).get("likeCreationDate")),   // likeCreationDate
                    Long.parseLong(result.get(i).get("messageId")),             // messageId
                    messageContent,
                    latency,
                    isNew
            );
            endResult.add(res);                                                 // add to result list
        }
        resultReporter.report(0, endResult, operation);
    }
}
