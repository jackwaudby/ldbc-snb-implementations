package com.jackwaudby.ldbcimplementations.queryhandlers;

import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery7MessageReplies;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery7MessageRepliesResult;

import java.util.ArrayList;
import java.util.HashMap;

import static com.jackwaudby.ldbcimplementations.utils.HttpResponseToResultList.httpResponseToResultList;

public class LdbcShortQuery7MessageRepliesHandler implements OperationHandler<LdbcShortQuery7MessageReplies, JanusGraphDb.JanusGraphConnectionState> {

    @Override
    public void executeOperation(LdbcShortQuery7MessageReplies operation, JanusGraphDb.JanusGraphConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {

        // originalMessage=g.V().has('Post','id',34359748472).fold().coalesce(unfold(),V().has('Comment','id',34359748472)).next()
        // originalAuthor=g.V(originalMessage).out('hasCreator').next()
        // replies=g.V(originalMessage).in('replyOf').order().by('creationDate',desc).by('id',asc).toList()
        // repliesAuthor=g.V(replies).out('hasCreator').as('replyAuthor').toList()
        // g.V(repliesAuthor).as('repliesAuthor').choose(bothE('knows').otherV().hasId(originalAuthor.id()),constant(true),constant(false)).as('knows').select('knows','repliesAuthor')
        // g.V().has('Post','id',34359748472).fold().coalesce(unfold(),V().has('Comment','id',34359748472)).as('originalMessage').in('replyOf').as('comment').order().by(select('comment').by('creationDate'),desc).by('id',asc).out('hasCreator').as('replyAuthor').choose(bothE('knows').otherV().hasId(originalAuthor.id()),constant(true),constant(false)).as('knows').select('comment','replyAuthor','knows').by(valueMap('id','content','creationDate')).by(valueMap('id','firstName','lastName')).by()


 // TODO: Add transaction logic to query string
 // TODO: Add transaction retry logic to response

 long messageId = operation.messageId();
    JanusGraphDb.JanusGraphClient client = dbConnectionState.getClient();   // janusgraph client
//    String queryString = "{\"gremlin\": \"" +                               // gremlin query string
//            "originalMessage=g.V().has('Post','id',"+messageId+").fold().coalesce(unfold(),V().has('Comment','id',"+messageId+")).next();" +
//            "originalAuthor=g.V(originalMessage).out('hasCreator').next();" +
//            "g.V(originalMessage).in('replyOf').as('comment').order().by(select('comment').by('creationDate'),desc).by('id',asc).out('hasCreator').as('replyAuthor').select('comment','replyAuthor').by(valueMap('id','content','creationDate')).by(valueMap('id','firstName','lastName'))" +
//            "\"" +
//            "}";
        String queryString = "{\"gremlin\": \"" +                               // gremlin query string
                "originalMessage=g.V().has('Post','id',"+messageId+").fold().coalesce(unfold(),V().has('Comment','id',"+messageId+")).next();" +
                "originalAuthor=g.V(originalMessage).out('hasCreator').next();" +
                "g.V(originalMessage).as('originalMessage').in('replyOf').as('comment').order().by(select('comment').by('creationDate'),desc).by('id',asc).out('hasCreator').as('replyAuthor').choose(bothE('knows').otherV().hasId(originalAuthor.id()),constant(true),constant(false)).as('knows').select('comment','replyAuthor','knows').by(valueMap('id','content','creationDate')).by(valueMap('id','firstName','lastName')).by(fold());" +
                "\"" +
                "}";
    String response = client.execute(queryString);                          // execute query
    ArrayList<HashMap<String, String>> result                               // parse result
            = httpResponseToResultList(response);
    ArrayList<LdbcShortQuery7MessageRepliesResult> endResult                   // init result list
                = new ArrayList<>();
    for (int i = 0; i < result.size(); i++) {                               // for each result
            LdbcShortQuery7MessageRepliesResult res                                    // create result object
                    = new LdbcShortQuery7MessageRepliesResult(
                    Long.parseLong(result.get(i).get("commentId")),
                    result.get(i).get("commentContent"),
                    Long.parseLong(result.get(i).get("commentCreationDate")),
                    Long.parseLong(result.get(i).get("replyAuthorId")),
                    result.get(i).get("replyAuthorFirstName"),
                    result.get(i).get("replyAuthorLastName"),
                    Boolean.parseBoolean(result.get(i).get("knows"))
            );
            endResult.add(res);                                                 // add to result list
        }
        resultReporter.report(0, endResult, operation);
    }
}

