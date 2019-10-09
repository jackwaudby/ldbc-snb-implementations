package com.jackwaudby.ldbcimplementations.queryhandlers;

import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery2PersonPostsResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery3PersonFriends;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery3PersonFriendsResult;

import java.util.ArrayList;
import java.util.HashMap;

import static com.jackwaudby.ldbcimplementations.utils.HttpResponseToResultList.httpResponseToResultList;
import static com.jackwaudby.ldbcimplementations.utils.HttpResponseToResultMap.httpResponseToResultMap;

public class LdbcShortQuery3PersonFriendsHandler implements OperationHandler<LdbcShortQuery3PersonFriends, JanusGraphDb.JanusGraphConnectionState> {

    @Override
    public void executeOperation(LdbcShortQuery3PersonFriends operation, JanusGraphDb.JanusGraphConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {

        // get query parameter from operation
        long personId = operation.personId();
        // get JanusGraph client
        JanusGraphDb.JanusGraphClient client = dbConnectionState.getClient();
        // gremlin query string
        String queryString = "{\"gremlin\": \"" +
                "g.V().has('Person','id'," + personId + ")" +
                ".bothE('knows').order().by('creationDate',decr).as('edge').otherV().as('friend').select('edge','friend').by(valueMap('creationDate')).by(valueMap('id','firstName','lastName')).toList();" +
                "\"}";
        String response = client.execute(queryString);                          // execute query
        ArrayList<HashMap<String, String>> result                               // parse result
                = httpResponseToResultList(response);
        ArrayList<LdbcShortQuery3PersonFriendsResult> endResult                   // init result list
                        = new ArrayList<>();
        for (int i = 0; i < result.size(); i++) {
            LdbcShortQuery3PersonFriendsResult res =
                    new LdbcShortQuery3PersonFriendsResult(
                            Long.parseLong(result.get(i).get("friendId")),
                            result.get(i).get("friendFirstName"),
                            result.get(i).get("friendLastName"),
                            Long.parseLong(result.get(i).get("edgeCreationDate"))
                    );
            endResult.add(res);
        }
        resultReporter.report(0, endResult, operation);
    }
}

