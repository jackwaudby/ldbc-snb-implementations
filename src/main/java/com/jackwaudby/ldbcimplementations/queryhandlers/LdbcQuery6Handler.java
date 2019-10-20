package com.jackwaudby.ldbcimplementations.queryhandlers;


import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery6;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery6Result;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;

import static com.jackwaudby.ldbcimplementations.utils.HttpResponseToResultList.httpResponseToResultList;

public class LdbcQuery6Handler implements OperationHandler<LdbcQuery6, JanusGraphDb.JanusGraphConnectionState> {

    @Override
    public void executeOperation(LdbcQuery6 operation, JanusGraphDb.JanusGraphConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {

        // TODO: Add transaction logic to query string
        // TODO: Add transaction retry logic to response

        long personId = operation.personId();
        String tagName = operation.tagName();
        int limit = operation.limit();

        JanusGraphDb.JanusGraphClient client = dbConnectionState.getClient();   // janusgraph client

        String queryString = "{\"gremlin\": \"" +                               // gremlin query string
                "g.V().has('Person','id',"+personId+").repeat(both('knows').simplePath()).emit().times(2).dedup()." +
                "in('hasCreator').hasLabel('Post')." +
                "where(out('hasTag').has('Tag','name','"+tagName+"'))." +
                "out('hasTag').has('Tag','name',neq('"+tagName+"'))." +
                "groupCount().by('name').order(local).by(values,desc).by(keys,asc).unfold().limit("+limit+").fold()" +
                "\"" +
                "}";
        String response = client.execute(queryString);                          // execute query
        // TODO: adjust query to fit into standard parser
        JSONObject responseJson = new JSONObject(response);                         // convert to JSON
        JSONArray results = responseJson.getJSONObject("result").                        // get results
                getJSONObject("data").
                getJSONArray("@value").
                getJSONObject(0).getJSONArray("@value");

        ArrayList<LdbcQuery6Result> endResult                                   // init result list
                = new ArrayList<>();

        for(int i = 0; i<results.length();i++) {
            String key = results.getJSONObject(i).getJSONArray("@value").getString(0);
            int value = results.getJSONObject(i).getJSONArray("@value").getJSONObject(1).getInt("@value");
            LdbcQuery6Result res = new LdbcQuery6Result(
                    key,
                    value
            );
            endResult.add(res);
        }

        resultReporter.report(0, endResult, operation);
    }
}

