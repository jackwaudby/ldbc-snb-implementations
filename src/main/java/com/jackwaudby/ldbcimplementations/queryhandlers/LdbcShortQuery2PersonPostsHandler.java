package com.jackwaudby.ldbcimplementations.queryhandlers;

import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery2PersonPosts;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery2PersonPostsResult;

import java.util.ArrayList;
import java.util.HashMap;

import static com.jackwaudby.ldbcimplementations.utils.HttpResponseToResultMap.httpResponseToResultMap;
import static com.jackwaudby.ldbcimplementations.utils.HttpResponseToResultList.httpResponseToResultList;
/**
 * Given a Person, retrieve the last 10 messages created by that user.
 * For each message, return the message id, content/imageFile and creationDate.
 * Then return the original post from its conversation and the author of that post, id, firstName and lastName.
 * If the message is a post then the original post will be the same message.
 */
public class LdbcShortQuery2PersonPostsHandler implements OperationHandler<LdbcShortQuery2PersonPosts, JanusGraphDb.JanusGraphConnectionState> {

    @Override
    public void executeOperation(LdbcShortQuery2PersonPosts operation, JanusGraphDb.JanusGraphConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {

        // TODO: Add transaction logic to query string
        // TODO: Add transaction retry logic to response

        long personId = operation.personId();                                   // start person
        JanusGraphDb.JanusGraphClient client = dbConnectionState.getClient();   // janusgraph client
        String queryString = "{\"gremlin\": \"" +                               // gremlin query string
                "g.V().has('Person','id'," + personId + ").inE('hasCreator').outV().order().by('creationDate',decr).limit(10).as('message').union(repeat(out('replyOf').simplePath()).until(hasLabel('Post')).as('originalPost').out('hasCreator').as('originalAuthor'),hasLabel('Post').as('originalPost').outE('hasCreator').inV().as('originalAuthor')).select('message','originalPost','originalAuthor').by(valueMap('id','imageFile','content','creationDate')).by(valueMap('id')).by(valueMap('id','firstName','lastName')).toList();" +
                "\"" +
                "}";
        String response = client.execute(queryString);                          // execute query
        ArrayList<HashMap<String, String>> result                               // parse result
                = httpResponseToResultList(response);
        ArrayList<LdbcShortQuery2PersonPostsResult> endResult                   // init result list
                = new ArrayList<>();
        for (int i = 0; i < result.size(); i++) {                               // for each result
            String messageContent;                                              // set message content
            if (result.get(i).get("content").equals("")) {                      // imagefile
                messageContent = result.get(i).get("imageFile");
            } else {                                                            // content
                messageContent = result.get(i).get("content");
            }
        LdbcShortQuery2PersonPostsResult res                                    // create result object
                = new LdbcShortQuery2PersonPostsResult(
                        Long.parseLong(result.get(i).get("messageId")),
                        messageContent,
                        Long.parseLong(result.get(i).get("messageCreationDate")),
                        Long.parseLong(result.get(i).get("originalPostId")),
                        Long.parseLong(result.get(i).get("originalAuthorId")),
                        result.get(i).get("originalAuthorFirstName"),
                        result.get(i).get("originalAuthorLastName")
        );
            endResult.add(res);                                                 // add to result list
        }
        resultReporter.report(0, endResult, operation);              // pass to result reporter

    }
}
