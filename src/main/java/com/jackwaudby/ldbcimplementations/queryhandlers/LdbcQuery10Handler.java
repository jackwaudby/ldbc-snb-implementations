package com.jackwaudby.ldbcimplementations.queryhandlers;

import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery10;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery10Result;
import org.json.JSONObject;

import java.util.ArrayList;

import static com.jackwaudby.ldbcimplementations.utils.GremlinResponseParsers.*;

/**
 * Given a start Person (rootPerson), find that Person’s friends of friends (person).
 * Exclude rootPerson, and immediate friends, who were born on or after the 21st of a given month (in any year) and
 * before the 22nd of the following month.
 * Calculate the similarity between each person and rootPerson, where commonInterestScore is defined as follows:
 * - common = number of Posts created by person, such that the Post has a Tag that rootPerson is interested in
 * - uncommon = number of Posts created by person, such that the Post has no Tag that rootPerson is interested in
 * - commonInterestScore = common - uncommon
 *
 * Return personId, personFirstName, personLastName, personGender, city person is located in (personCityName) and
 * commonInterestScore. Sorted by commonInterestScore (DESC) and personId (ASC). Limit 10
 */
public class LdbcQuery10Handler implements OperationHandler<LdbcQuery10, JanusGraphDb.JanusGraphConnectionState> {

    @Override
    public void executeOperation(LdbcQuery10 operation, JanusGraphDb.JanusGraphConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {

        // TODO: Add transaction logic to query string
        // TODO: Add transaction retry logic to response

        long personId = operation.personId();
        long month = operation.month();
        long nextMonth;
        if (month != 12 ) {
            nextMonth = month + 1;
        } else {
            nextMonth = 1;
        }
        long limit = operation.limit();


        JanusGraphDb.JanusGraphClient client = dbConnectionState.getClient();   // janusgraph client

        String queryString = "{\"gremlin\": \"" +                               // gremlin query string
                "g.V().has('Person','id',"+personId+").sideEffect(out('hasInterest').store('tags'))." +
                "repeat(both('knows').simplePath()).times(2).dedup()." +
                "where(values('birthday').map{it.get().getMonth()}.is(eq("+month+"-1))." +
                "and().values('birthday').map{it.get().getDate()}.is(gte(21))." +
                "or().values('birthday').map{it.get().getMonth()}.is(eq("+nextMonth+"-1))." +
                "and().values('birthday').map{it.get().getDate()}.is(lt(22)))." +
                "local(union(__.in('hasCreator').hasLabel('Post').where(out('hasTag').dedup()." +
                "where(within('tags')).count().is(gt(0))).count()," +
                "__.in('hasCreator').hasLabel('Post').count()," +
                "values('id','firstName','lastName','gender')," +
                "out('isLocatedIn').values('name')).fold())." +
                "map{it -> [" +
                "commonInterestScore:[it.get().getAt(0) - (it.get().getAt(1)-it.get().getAt(0))]," +
                "id:[it.get().getAt(2)], " +
                "firstName:[it.get().getAt(3)]," +
                "lastName:[it.get().getAt(4)], " +
                "gender:[it.get().getAt(5)], " +
                "city:[it.get().getAt(6)],]}." +
                "order().by(select('commonInterestScore').unfold(),desc).by(select('id').unfold(),asc).limit("+limit+")" +
                "\"" +
                "}";
        String response = client.execute(queryString);                          // execute query
        ArrayList<JSONObject> resultList = gremlinResponseToResultArrayList(response);
        ArrayList<LdbcQuery10Result> endResult                                   // init result list
                = new ArrayList<>();
        for (JSONObject result: resultList) {
            Long id = Long.parseLong(getPropertyValue(gremlinMapToHashMap(result).get("id")));
            LdbcQuery10Result res                                                // create result object
                    = new LdbcQuery10Result(
                            id,
                    getPropertyValue(gremlinMapToHashMap(result).get("firstName")),
                    getPropertyValue(gremlinMapToHashMap(result).get("lastName")),
                    Integer.parseInt(getPropertyValue(gremlinMapToHashMap(result).get("commonInterestScore"))),
                    getPropertyValue(gremlinMapToHashMap(result).get("gender")),
                    getPropertyValue(gremlinMapToHashMap(result).get("city"))
                    );
            endResult.add(res);
        }
        resultReporter.report(0, endResult, operation);
    }
}


// TODO: set_01
// TODO: set_02





