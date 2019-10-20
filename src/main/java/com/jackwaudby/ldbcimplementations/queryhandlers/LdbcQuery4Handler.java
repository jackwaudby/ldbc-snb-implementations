package com.jackwaudby.ldbcimplementations.queryhandlers;

import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery4;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery4Result;
import org.json.JSONArray;
import org.json.JSONObject;

import javax.crypto.spec.PSource;
import java.util.ArrayList;
import java.util.HashMap;

import static com.jackwaudby.ldbcimplementations.utils.HttpResponseToResultList.httpResponseToResultList;

public class LdbcQuery4Handler implements OperationHandler<LdbcQuery4, JanusGraphDb.JanusGraphConnectionState> {

    @Override
    public void executeOperation(LdbcQuery4 operation, JanusGraphDb.JanusGraphConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {

        // TODO: Add transaction logic to query string
        // TODO: Add transaction retry logic to response

        long personId = operation.personId();
        long startDate = operation.startDate().getTime();
        long duration = (operation.durationDays() * 24L * 60L * 60L * 1000L);
        long endDate = startDate + duration;
        long limit = operation.limit();



        JanusGraphDb.JanusGraphClient client = dbConnectionState.getClient();   // janusgraph client

        String queryString = "{\"gremlin\": \"" +                               // gremlin query string
                "g.V().has('Person','id',"+personId+").both('knows').in('hasCreator').sideEffect(has('Post','creationDate',lt(new Date("+startDate+"))).out('hasTag').aggregate('oldTags')).has('Post','creationDate',between(new Date("+startDate+"),new Date("+endDate+"))).out('hasTag').where(without('oldTags')).order().by('name').group().by('name').by(count()).order(local).by(values,desc).by(keys,asc).unfold().limit("+limit+").fold()" +
                "\"" +
                "}";
        String response = client.execute(queryString);                          // execute query
        // TODO: adjust query to fit into standard parser
        JSONObject responseJson = new JSONObject(response);                         // convert to JSON
        JSONArray results = responseJson.getJSONObject("result").                        // get results
                getJSONObject("data").
                getJSONArray("@value").
                getJSONObject(0).getJSONArray("@value");

        ArrayList<LdbcQuery4Result> endResult                                   // init result list
                = new ArrayList<>();
        for(int i = 0; i<results.length();i++) {
            String key = results.getJSONObject(i).getJSONArray("@value").getString(0);
            int value = results.getJSONObject(i).getJSONArray("@value").getJSONObject(1).getInt("@value");
            LdbcQuery4Result res = new LdbcQuery4Result( key,value);
            endResult.add(res);
        }
        resultReporter.report(0, endResult, operation);

    }
}


