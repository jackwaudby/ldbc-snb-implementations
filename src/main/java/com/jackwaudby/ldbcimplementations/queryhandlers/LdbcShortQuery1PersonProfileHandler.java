package com.jackwaudby.ldbcimplementations.queryhandlers;

import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import com.jackwaudby.ldbcimplementations.utils.ParseDateLong;
import com.jackwaudby.ldbcimplementations.utils.ParseResultMap;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery1PersonProfile;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery1PersonProfileResult;

import java.text.ParseException;
import java.util.HashMap;

/**
 * Given a start Person, retrieve their first name, last name, birthday, IP
 * address, browser, and city of residence.
 */
public class LdbcShortQuery1PersonProfileHandler implements OperationHandler<LdbcShortQuery1PersonProfile, JanusGraphDb.JanusGraphConnectionState> {

    //TODO: Add transaction and retry logic

    @Override
    public void executeOperation(LdbcShortQuery1PersonProfile operation, JanusGraphDb.JanusGraphConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {

        // get query parameter from operation
        long personId = operation.personId();
        // get JanusGraph client
        JanusGraphDb.JanusGraphClient client = dbConnectionState.getClient();
        // gremlin query string
        String queryString = "{\"gremlin\": \"def v = g.V().has('Person','id'," + personId + ").next();[];def hm = g.V(v).valueMap('firstName','lastName','birthday','locationIP','browserUsed','gender','creationDate').next();[];def v2 = g.V(v).outE('isLocatedIn').inV().valueMap('id').next();[]; def cityId = v2['id'];[];hm.put('cityId',cityId);[];hm.toString()\"}";

        try {
            // execute query TODO: handle error case
            String result = client.execute(queryString);
            // convert result string into map
            HashMap<String, String> resultMap = ParseResultMap.resultToMap(result);
            // create result for driver
            LdbcShortQuery1PersonProfileResult endResult = new LdbcShortQuery1PersonProfileResult(
                    resultMap.get("firstName"),
                    resultMap.get("lastName"),
                    ParseDateLong.birthdayStringToLong(resultMap.get("birthday")),
                    resultMap.get("locationIP"),
                    resultMap.get("browserUsed"),
                    Long.parseLong(resultMap.get("cityId")),
                    resultMap.get("gender"),
                    ParseDateLong.creationDateStringToLong(resultMap.get("creationDate")));
            // pass result to driver
            resultReporter.report(0, endResult, operation);
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}
