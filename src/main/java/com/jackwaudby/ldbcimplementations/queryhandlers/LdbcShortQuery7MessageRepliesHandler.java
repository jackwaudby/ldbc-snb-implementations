package com.jackwaudby.ldbcimplementations.queryhandlers;

import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import com.jackwaudby.ldbcimplementations.QueryTestBed;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcNoResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery7MessageReplies;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery7MessageRepliesResult;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import java.util.ArrayList;

import static com.jackwaudby.ldbcimplementations.utils.GremlinResponseParsers.*;
import static com.jackwaudby.ldbcimplementations.utils.ImplementationConfiguration.getTxnAttempts;

/**
 * Given a Message, retrieve the (1-hop) Comments that reply to it.
 * In addition, return a boolean flag knows indicating if the author of the reply knows the author of the original message.
 * If author is same as original author, return false for knows flag.
 */
public class LdbcShortQuery7MessageRepliesHandler implements OperationHandler<LdbcShortQuery7MessageReplies, JanusGraphDb.JanusGraphConnectionState> {

    private static Logger LOGGER = Logger.getLogger(LdbcShortQuery7MessageRepliesHandler.class.getName());


    @Override
    public void executeOperation(LdbcShortQuery7MessageReplies operation, JanusGraphDb.JanusGraphConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {

        long messageId = operation.messageId();
        JanusGraphDb.JanusGraphClient client = dbConnectionState.getClient();   // janusgraph client
        String queryString = "{\"gremlin\": \"" +                               // gremlin query string
                "graph.tx().rollback();[];" +
                "try{" +
                "originalMessage=g.V().has('Post','id',"+messageId+").fold().coalesce(unfold(),V().has('Comment','id',"+messageId+")).next();[];" +
                "originalAuthor=g.V(originalMessage).out('hasCreator').next();[];" +
                "result=g.V(originalMessage).as('originalMessage').in('replyOf').as('comment')." +
                "order().by(select('comment').by('creationDate'),desc).by(select('comment').by('id'),asc)." +
                "out('hasCreator').as('replyAuthor')." +
                "choose(bothE('knows').otherV().hasId(originalAuthor.id()),constant(true),constant(false)).as('knows')." +
                "select('comment','replyAuthor','knows')." +
                "by(valueMap('id','content','creationDate')).by(valueMap('id','firstName','lastName')).by(fold())." +
                "map{it -> [" +
                "commentId:it.get().get('comment').get('id')," +
                "commentContent:it.get().get('comment').get('content')," +
                "commentCreationDate:it.get().get('comment').get('creationDate')," +
                "replyAuthorId:it.get().get('replyAuthor').get('id')," +
                "replyAuthorFirstName:it.get().get('replyAuthor').get('firstName')," +
                "replyAuthorLastName:it.get().get('replyAuthor').get('lastName')," +
                "replyAuthorKnowsOriginalMessageAuthor:it.get().get('knows')" +
                "]};[];" +
                "graph.tx().commit();[];"+
                "} catch (Exception e) {"+
                "errorMessage =[e.toString()];[];" +
                "result=[error:errorMessage];" +
                "graph.tx().rollback();[];" +
                "};" +
                "result" +
                "\"" +
                "}";

        int TX_ATTEMPTS = 0;                                                                 // init. transaction attempts
        int TX_RETRIES = getTxnAttempts();

        while (TX_ATTEMPTS < TX_RETRIES) {
            LOGGER.info("Attempt " + (TX_ATTEMPTS + 1) + ": " + LdbcShortQuery7MessageRepliesHandler.class.getSimpleName());
            String response = client.execute(queryString);                                            // execute query
            ArrayList<JSONObject> results = gremlinResponseToResultArrayList(response);          // get result list
            if (gremlinMapToHashMap(results.get(0)).containsKey("error")) {
                LOGGER.error(getPropertyValue(gremlinMapToHashMap(results.get(0)).get("error")));
                TX_ATTEMPTS = TX_ATTEMPTS + 1;
            } else if (true) {
                ArrayList<LdbcShortQuery7MessageRepliesResult> queryResults                   // init result list
                    = new ArrayList<>();
                LOGGER.info("B");

                LdbcShortQuery7MessageRepliesResult queryResult                                    // create result object
                        = new LdbcShortQuery7MessageRepliesResult(
                        11111,
                        "skd",
                        112323,
                        2222,
                        "replyAuthorFirstName",
                        "replyAuthorLastName",
                        true
                );
                queryResults.add(queryResult);

            }
            else
                {
                    LOGGER.info("C");

                    ArrayList<LdbcShortQuery7MessageRepliesResult> queryResults                   // init result list
                        = new ArrayList<>();
                for (JSONObject result: results
                ) {
                    long commentId = Long.parseLong(getPropertyValue(gremlinMapToHashMap(result).get("commentId")));
                    String commentContent = getPropertyValue(gremlinMapToHashMap(result).get("commentContent"));
                    long commentCreationDate = Long.parseLong(getPropertyValue(gremlinMapToHashMap(result).get("commentCreationDate")));
                    long replyAuthorId = Long.parseLong(getPropertyValue(gremlinMapToHashMap(result).get("replyAuthorId")));
                    String replyAuthorFirstName = getPropertyValue(gremlinMapToHashMap(result).get("replyAuthorFirstName"));
                    String replyAuthorLastName = getPropertyValue(gremlinMapToHashMap(result).get("replyAuthorLastName"));
                    boolean replyAuthorKnowsOriginalMessageAuthor = gremlinMapToHashMap(result).get("replyAuthorKnowsOriginalMessageAuthor").getJSONArray("@value").getBoolean(0);
                    LdbcShortQuery7MessageRepliesResult queryResult                                    // create result object
                            = new LdbcShortQuery7MessageRepliesResult(
                            commentId,
                            commentContent,
                            commentCreationDate,
                            replyAuthorId,
                            replyAuthorFirstName,
                            replyAuthorLastName,
                            replyAuthorKnowsOriginalMessageAuthor
                    );
                    queryResults.add(queryResult);
                }

                LOGGER.info("Number of results: " + queryResults.size());

                resultReporter.report(0, queryResults, operation);



                break;
            }
        }
    }
}

