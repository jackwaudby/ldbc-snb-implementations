package com.jackwaudby.ldbcimplementations.queryhandlers;

import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery11;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery11Result;

import java.util.ArrayList;
import java.util.HashMap;

import static com.jackwaudby.ldbcimplementations.utils.HttpResponseToResultList.httpResponseToResultList;

/**
 * Given a start Person, find that Person’s friends and friends of friends (excluding start Person)
 * who started Working in some Company in a given Country, before a given date (year).
 */
public class LdbcQuery11Handler implements OperationHandler<LdbcQuery11, JanusGraphDb.JanusGraphConnectionState> {

    @Override
    public void executeOperation(LdbcQuery11 operation, JanusGraphDb.JanusGraphConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {

        // TODO: Add transaction logic to query string
        // TODO: Add transaction retry logic to response

        long personId = operation.personId();
        String countryName = operation.countryName();
        int workFromYear = operation.workFromYear();

        JanusGraphDb.JanusGraphClient client = dbConnectionState.getClient();   // janusgraph client

        String queryString = "{\"gremlin\": \"" +                               // gremlin query string
                "g.V().has('Person','id',"+personId+")." +
                "repeat(both('knows').simplePath()).emit().times(2).dedup().as('person')." +
                "outE('workAt').has('workFrom',lt("+workFromYear+")).as('organisationYear')." +
                "inV().as('organisation')." +
                "out('isLocatedIn').has('name','"+countryName+"')." +
                "order()." +
                    "by(select('organisationYear').by('workFrom'))." +
                    "by(select('person').by('id'))." +
                    "by(select('organisation').by('name'),desc)." +
                "select('person','organisation','organisationYear')." +
                    "by(valueMap('id','firstName','lastName'))." +
                    "by(valueMap('name')).by(valueMap('workFrom'))" +
                "\"" +
                "}";
        String response = client.execute(queryString);                          // execute query
        ArrayList<HashMap<String, String>> result                               // parse result
                = httpResponseToResultList(response);
        ArrayList<LdbcQuery11Result> endResult                                   // init result list
                = new ArrayList<>();
        for (int i = 0; i < result.size(); i++) {                               // for each result
            LdbcQuery11Result res                                                // create result object
                    = new LdbcQuery11Result(
                    Long.parseLong(result.get(i).get("personId")),              // personId
                    result.get(i).get("personFirstName"),                       // personFirstName
                    result.get(i).get("personLastName"),                        // personLastName
                    result.get(i).get("organisationName"),
                    Integer.parseInt(result.get(i).get("organisationYearWorkFrom"))
            );
            endResult.add(res);                                                 // add to result list
        }
        resultReporter.report(0, endResult, operation);
    }
}

