package com.jackwaudby.ldbcimplementations.queryhandlers;


import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery12;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery12Result;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.jackwaudby.ldbcimplementations.utils.GremlinResponseParsers.*;

/**
 * Given a start Person, find the Comments that this Person’s friends made in reply to Posts.
 * Considering only those Comments that are immediate (1-hop) replies to Posts, not the transitive (multi-hop) case.
 * Only consider Posts with a Tag in a given TagClass or in a descendent of that TagClass.
 * Count the number of these reply Comments.
 * Collect the Tags that were attached to the Posts they replied to,
 *  but only collect Tags with the given TagClass or with a descendant of that TagClass
 * Return Persons with at least one reply, the reply count, and the collection of Tags.
 */

public class LdbcQuery12Handler implements OperationHandler<LdbcQuery12, JanusGraphDb.JanusGraphConnectionState> {

    @Override
    public void executeOperation(LdbcQuery12 operation, JanusGraphDb.JanusGraphConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {

        // TODO: Add transaction logic to query string
        // TODO: Add transaction retry logic to response

        long personId = operation.personId();
        long limit = operation.limit();
        String tagClassName = operation.tagClassName();

        JanusGraphDb.JanusGraphClient client = dbConnectionState.getClient();   // janusgraph client

        String queryString = "{\"gremlin\": \"" +                               // gremlin query string
                "g.V().has('Person','id',"+personId+").both('knows')." +
                "  where(" +
                "    local(" +
                "      __.in('hasCreator')." +
                "        where(out('replyOf').hasLabel('Post').out('hasTag').out('hasType').has('name','"+tagClassName+"'))." +
                "        out('replyOf').out('hasTag').count().is(gt(0))))." +
                "  order()." +
                "    by(local(__.in('hasCreator')." +
                "      where(out('replyOf').hasLabel('Post').out('hasTag').out('hasType').has('name','"+tagClassName+"'))." +
                "      out('replyOf').out('hasTag').count()),desc)." +
                "    by('id',asc)." +
                "  local(union(" +
                "    __.in('hasCreator').out('replyOf').hasLabel('Post').out('hasTag')." +
                "      where(out('hasType').has('name','"+tagClassName+"'))." +
                "      count().fold()," +
                "    valueMap('id','firstName','lastName').unfold(),"+
                "    __.in('hasCreator').out('replyOf').hasLabel('Post').out('hasTag')." +
                "      where(out('hasType').has('name','"+tagClassName+"'))."+
                "      values('name').fold()" +
                "  ).fold())" +
                "\"" +
                "}";
//    g.V().has('Person','id',8796093023661).both('knows').where(local(__.in('hasCreator').where(out('replyOf').hasLabel('Post').out('hasTag').out('hasType').has('name','AmericanFootballPlayer')).out('replyOf').out('hasTag').count().is(gt(0)))).local(union(__.in('hasCreator').out('replyOf').hasLabel('Post').out('hasTag').where(out('hasType').has('name','AmericanFootballPlayer')).count().fold(),valueMap('id','firstName','lastName').unfold(),__.in('hasCreator').out('replyOf').hasLabel('Post').out('hasTag').where(out('hasType').has('name','AmericanFootballPlayer')).values('name').fold()).fold())

        String response = client.execute(queryString);                          // execute query
        ArrayList<JSONObject> resultList = gremlinResponseToResultArrayList(response);

        ArrayList<LdbcQuery12Result> endResult                                   // init result list
                = new ArrayList<>();
        ArrayList<JSONObject> results = gremlinResponseToResultArrayList(response);
        if (results.size() != 0) {
            for (JSONObject result : results) {
                        String tagNames = getPropertyValue(gremlinListToArrayList(result).get(4));

                        List<String> tagNamesList =
                        Arrays.asList(
                                tagNames.replaceAll("[\\[\\]\\s+]", "").split(","));
                if (tagNamesList.size() == 1 && tagNamesList.get(0).equals("")) {
                    tagNamesList = new ArrayList<>();
                }

                        LdbcQuery12Result res                                                // create result object
                        = new LdbcQuery12Result(
                        Long.parseLong(getPropertyValue(gremlinMapToHashMap(gremlinListToArrayList(result).get(1)).get("id"))),
                        getPropertyValue(gremlinMapToHashMap(gremlinListToArrayList(result).get(2)).get("firstName")),
                        getPropertyValue(gremlinMapToHashMap(gremlinListToArrayList(result).get(3)).get("lastName")),
                        tagNamesList,
                        Integer.parseInt(getPropertyValue(gremlinListToArrayList(result).get(0)))
                        );
                endResult.add(res);
            }
        }
        resultReporter.report(0, endResult, operation);


    }
}





