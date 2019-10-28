package com.jackwaudby.ldbcimplementations.queryhandlers;

import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery13;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery13Result;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;

import static com.jackwaudby.ldbcimplementations.utils.GremlinResponseParsers.getPropertyValue;
import static com.jackwaudby.ldbcimplementations.utils.GremlinResponseParsers.gremlinResponseToResultArrayList;

/**
 * Title: Single shortest path
 *
 * Description: Given two Persons, find the shortest path between these two Persons in the subgraph induced
 * by the Knows relationships.
 */
public class LdbcQuery13Handler implements OperationHandler<LdbcQuery13, JanusGraphDb.JanusGraphConnectionState> {

    @Override
    public void executeOperation(LdbcQuery13 operation, JanusGraphDb.JanusGraphConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {

        // TODO: Add transaction logic to query string
        // TODO: Add transaction retry logic to response

        long person1Id = operation.person1Id();
        long person2Id = operation.person2Id();

        JanusGraphDb.JanusGraphClient client = dbConnectionState.getClient();   // janusgraph client

        String queryString = "{\"gremlin\": \"" +                               // gremlin query string
                "g.V().has('Person','id',"+person1Id+")." +
                "choose(" +
                "repeat(both('knows').dedup()).until(has('Person','id',"+person2Id+")).limit(1).path().count(local).is(gt(0))," +
                "repeat(store('x').both('knows').where(without('x')).aggregate('x')).until(has('Person','id',"+person2Id+")).limit(1).path().count(local)," +
                "constant(-1)).fold()" +
                "\"" +
                "}";
        String response = client.execute(queryString);                          // execute query
        ArrayList<JSONObject> resultList = gremlinResponseToResultArrayList(response);
        Integer shortestPathLength = Integer.parseInt(getPropertyValue(resultList.get(0)));
        if (shortestPathLength != - 1 ){
            shortestPathLength = shortestPathLength - 1;
        }
                                                        // for each result
        LdbcQuery13Result endResult                                                // create result object
                    = new LdbcQuery13Result(
                            shortestPathLength
            );
        resultReporter.report(0, endResult, operation);


    }
}