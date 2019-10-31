package com.jackwaudby.ldbcimplementations.queryhandlers;

import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery14Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery14;
import org.json.JSONObject;

import java.util.ArrayList;

import static com.jackwaudby.ldbcimplementations.utils.GremlinResponseParsers.*;

/**
 * Given two Persons, find all (unweighted) shortest paths between these two Persons,
 * in the sub- graph induced by the Knows relationship.
 * Then, for each path calculate a weight.
 * The nodes in the path are Persons, and the weight of a path is the sum of weights between
 * every pair of consecutive Person nodes in the path.
 * The weight for a pair of Persons is calculated such that every reply (by one of the Persons) to
 * a Post (by the other Person) contributes 1.0, and every reply (by ones of the Persons) to
 * a Comment (by the other Person) contributes 0.5.
 * Return all the paths with shortest length, and their weights.
 * Do not return any rows if there is no path between the two Persons.
 */
public class LdbcQuery14Handler  implements OperationHandler<LdbcQuery14, JanusGraphDb.JanusGraphConnectionState> {

    @Override
    public void executeOperation(LdbcQuery14 operation, JanusGraphDb.JanusGraphConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {

        // TODO: Add transaction logic to query string
        // TODO: Add transaction retry logic to response
        // TODO: get all shortest paths of a given length
        // TODO: sort by path length

        long person1Id = operation.person1Id();
        long person2Id = operation.person2Id();

        JanusGraphDb.JanusGraphClient client = dbConnectionState.getClient();   // janusgraph client

        String queryString = "{\"gremlin\": \"" +                               // gremlin query string
                "g.withSack(0).V().has('Person','id',"+person1Id+")." +
                "repeat(__.as('pX').store('x').both('knows').where(without('x')).as('pY').aggregate('x').sack(sum).by(union(" +
                "match(__.as('pX').in('hasCreator').hasLabel('Post').as('post').in('replyOf').as('reply').out('hasCreator').as('pY')).count()," +
                "match(__.as('pX').in('hasCreator').hasLabel('Comment').as('post').in('replyOf').as('reply').out('hasCreator').as('pY')).count()," +
                "match(__.as('pY').in('hasCreator').hasLabel('Post').as('post').in('replyOf').as('reply').out('hasCreator').as('pX')).count()," +
                "match(__.as('pY').in('hasCreator').hasLabel('Comment').as('post').in('replyOf').as('reply').out('hasCreator').as('pX')).count()" +
                ").fold()." +
                "map{it -> (it.get().getAt(0) + it.get().getAt(2)) + ((it.get().getAt(1) + it.get().getAt(3)) / 2) }))." +
                "until(has('Person','id',"+person2Id+")).union(path().by('id'),sack()).fold().map{it -> [personIdsInPath: it.get().getAt(0), pathWeight: [it.get().getAt(1)]]}" +
                "\"" +
                "}";
        String response = client.execute(queryString);                          // execute query
        ArrayList<LdbcQuery14Result> endResult = new ArrayList<>();
        ArrayList<JSONObject> resultList = gremlinResponseToResultArrayList(response);
        for (JSONObject result: resultList) {
            Float pathWeight = Float.parseFloat(getPropertyValue(gremlinMapToHashMap(result).get("pathWeight")));
            ArrayList<JSONObject> personIdsInPath = gremlinListToArrayList(gremlinMapToHashMap(result).get("personIdsInPath").getJSONObject("@value").getJSONObject("objects"));
            ArrayList<Long> pathResult = new ArrayList<>();
            for (JSONObject node: personIdsInPath
            ) {
                pathResult.add(node.getLong("@value"));
            }
            LdbcQuery14Result res
                    = new LdbcQuery14Result(
                    pathResult,
                    pathWeight
            );
            endResult.add(res);
        }

        resultReporter.report(0, endResult, operation);


    }
}



//    g.withSack(0).V().has('Person','id',1616).
//    repeat(__.as('pX').store('x').both('knows').where(without('x')).as('pY').aggregate('x').sack(sum).by(union(
//            match(__.as('pX').in('hasCreator').hasLabel('Post').as('post').in('replyOf').as('reply').out('hasCreator').as('pY')).count(),
//    match(__.as('pX').in('hasCreator').hasLabel('Comment').as('post').in('replyOf').as('reply').out('hasCreator').as('pY')).count(),
//    match(__.as('pY').in('hasCreator').hasLabel('Post').as('post').in('replyOf').as('reply').out('hasCreator').as('pX')).count(),
//    match(__.as('pY').in('hasCreator').hasLabel('Comment').as('post').in('replyOf').as('reply').out('hasCreator').as('pX')).count()
//).fold().
//    map{it ->
//            (it.get().getAt(0) + it.get().getAt(2)) +
//                    ((it.get().getAt(1) + it.get().getAt(3)) / 2)
//    })).
//    until(has('Person','id',6597069767283)).union(path().by('id'),sack()).fold().
//    map{it -> [
//            personIdsInPath: it.get().getAt(0),
//            pathWeight: [it.get().getAt(1)]
//]}

