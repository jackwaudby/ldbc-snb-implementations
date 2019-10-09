package com.jackwaudby.ldbcimplementations.queryhandlers;

import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery6MessageForum;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery6MessageForumResult;

import java.util.ArrayList;
import java.util.HashMap;

import static com.jackwaudby.ldbcimplementations.utils.HttpResponseToResultList.httpResponseToResultList;

/**
 * Given a Message ID, retrieve the Forum ID and title that contains it and
 * retrieve the ID, firstName and lastName of the Person that moderates the Forum.
 */
public class LdbcShortQuery6MessageForumHandler implements OperationHandler<LdbcShortQuery6MessageForum, JanusGraphDb.JanusGraphConnectionState> {

    @Override
    public void executeOperation(LdbcShortQuery6MessageForum operation, JanusGraphDb.JanusGraphConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {

        // TODO: Add transaction logic to query string
        // TODO: Add transaction retry logic to response
        long messageId = operation.messageId();
        JanusGraphDb.JanusGraphClient client = dbConnectionState.getClient();   // janusgraph client
        String queryString = "{\"gremlin\": \"" +                               // gremlin query string
                "g.V().has('Post','id',"+messageId+").fold().coalesce(unfold(),V().has('Comment','id',"+messageId+").repeat(out('replyOf').simplePath()).until(hasLabel('Post'))).in('containerOf').as('forum').out('hasModerator').as('moderator').select('forum','moderator').by(valueMap('id','title')).by(valueMap('id','firstName','lastName'))" +
                "\"" +
                "}";
        String response = client.execute(queryString);                          // execute query
        ArrayList<HashMap<String, String>> result                               // parse result
                = httpResponseToResultList(response);

        LdbcShortQuery6MessageForumResult endResult = new LdbcShortQuery6MessageForumResult(
                Long.parseLong(result.get(0).get("forumId")),
                result.get(0).get("forumTitle"),
                Long.parseLong(result.get(0).get("moderatorId")),
                result.get(0).get("moderatorFirstName"),
                result.get(0).get("moderatorLastName")
                );
        resultReporter.report(0, endResult, operation);
    }
}
