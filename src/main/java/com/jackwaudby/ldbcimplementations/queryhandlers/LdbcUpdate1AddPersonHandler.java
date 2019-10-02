package com.jackwaudby.ldbcimplementations.queryhandlers;

import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcNoResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate1AddPerson;

import java.util.HashMap;
import java.util.List;

import static com.jackwaudby.ldbcimplementations.utils.HttpResponseToResultMap.httpResponseToResultMap;

public class LdbcUpdate1AddPersonHandler implements OperationHandler<LdbcUpdate1AddPerson,JanusGraphDb.JanusGraphConnectionState> {

    @Override
    public void executeOperation(LdbcUpdate1AddPerson operation, JanusGraphDb.JanusGraphConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {

        long personId = operation.personId();
        String firstName = operation.personFirstName();
        String lastName = operation.personLastName();
        String gender = operation.gender();
        long birthday = operation.birthday().getTime();
        long creationDate = operation.creationDate().getTime();
        String locationIP = operation.locationIp();
        String browserUsed = operation.browserUsed();
        long cityId = operation.cityId();
        List<String> languages = operation.languages();
        String lang = languages.toString().replaceAll(", ","', '").replaceAll( "\\[","\\['").replaceAll( "]","']");
        List<String> email = operation.emails();
        String em = email.toString().replaceAll(", ","', '").replaceAll( "\\[","\\['").replaceAll( "]","']");
//        List<Long> tagIds = operation.tagIds();

        // get JanusGraph client
        JanusGraphDb.JanusGraphClient client = dbConnectionState.getClient();
        // gremlin query string
        String queryString = "{\"gremlin\": \"try {" +
                "p = g.addV('Person')" +
                ".property('id'," + personId + ")" +
                ".property('firstName','" + firstName + "')" +
                ".property('lastName','" + lastName  + "')" +
                ".property('gender','" + gender  + "')" +
                ".property('birthday','" + birthday  + "')" +
                ".property('creationDate','" + creationDate  + "')" +
                ".property('locationIP','" + locationIP  + "')" +
                ".property('browserUsed','"+ browserUsed +"').next();[];" +
                "g.V().has('Place', 'id',"+ cityId +").as('city').V(p).addE('isLocatedIn').to('city').next();[];" +
                "languages="+lang+";[];"+
                "for (item in languages) { " +
                " g.V(p).property(set, 'language', item).next();[];" +
                "}; "+
                "email="+em+";[];"+
                "for (item in email) { " +
                "g.V(p).property(set, 'email', item).next();[];" +
                "}; "+
//                "tagid=[139, 205, 286, 470, 538];[];"+
//                "for (item in tagid) { " +
//                "g.V().has('Tag', 'id', item).as('tag').V(p).addE('hasInterest').to('tag').next();[];" +
//                "}; "+
                "graph.tx().commit();[];" +
                "queryOutcome=['success'];[];" +
                "hm=[query_outcome:queryOutcome];[];" +
                "} catch (Exception e) {" +
                "errorMessage =[e.toString()];[];" +
                "hm=[query_error:errorMessage];[];" +
                "graph.tx().rollback();[];" +
                "};" +
                "hm;\"" +
                "}";

        int TX_ATTEMPTS = 0;
        int TX_RETRIES = 5;

        while (TX_ATTEMPTS < TX_RETRIES) {
            System.out.println("Attempt " + (TX_ATTEMPTS + 1));
            String response = client.execute(queryString);                                // get response as string
            HashMap<String, String> result = httpResponseToResultMap(response);      // convert to result map
            if (result.containsKey("query_error")) {
                TX_ATTEMPTS = TX_ATTEMPTS + 1;
                System.out.println("Query Error: " + result.get("query_error"));
            } else if (result.containsKey("http_error")) {
                TX_ATTEMPTS = TX_ATTEMPTS + 1;
                System.out.println("Gremlin Server Error: " + result.get("http_error"));
            } else {
                System.out.println(result.get("query_outcome"));
                break;
            }
        }


            // pass result to driver
            resultReporter.report(0, LdbcNoResult.INSTANCE, operation);

    }
}
