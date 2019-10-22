package com.jackwaudby.ldbcimplementations.queryhandlers;

import com.jackwaudby.ldbcimplementations.JanusGraphDb;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery1;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery1Result;
import org.json.JSONObject;

import java.util.*;

import static com.jackwaudby.ldbcimplementations.utils.GremlinResponseParsers.*;

/**
 * Title: Friends with certain name
 *
 * Description: Given a start Person, find Persons with a given first name that the start Person is connected to
 * (excluding start Person) by at most 3 steps via Knows relationships. Return Persons, including the distance (1..3),
 * summaries of the Persons workplaces and places of study; sorted by distanceFromPerson (asc),
 * personLastName (asc), personId (asc).
 */
public class LdbcQuery1Handler implements OperationHandler<LdbcQuery1, JanusGraphDb.JanusGraphConnectionState> {

    @Override
    public void executeOperation(LdbcQuery1 operation, JanusGraphDb.JanusGraphConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {

        // TODO: Add transaction logic to query string
        // TODO: Add transaction retry logic to response

        long personId = operation.personId();                       // person ID
        String personFirstName = operation.firstName();             // person first name
        int limit = operation.limit();                              // result limit

        JanusGraphDb.JanusGraphClient client = dbConnectionState.getClient();   // janusgraph client

        String queryString = "{\"gremlin\": \"" +                               // gremlin query string
                "g.withSack(0).V().has('Person','id',"+personId+")." +
                "repeat(both('knows').simplePath().sack(sum).by(constant(1))).emit().times(3)." +
                "dedup().has('Person','firstName','"+personFirstName+"')." +
                "order().by(sack(),asc).by('lastName',asc).by('id',asc).limit("+limit+")" +
                "local(union(" +
                "valueMap('lastName','id','email','birthday','creationDate','gender','browserUsed','locationIP','speaks'), " +
                "out('isLocatedIn').valueMap('name')," +
                "sack().fold(), " +
                "outE('workAt').as('workFrom').inV().as('company').out('isLocatedIn').as('country')." +
                "select('company','workFrom','country').by(valueMap('name')).by(valueMap()).fold()," +
                "outE('studyAt').as('studyFrom').inV().as('university').out('isLocatedIn').as('country')." +
                "select('university','studyFrom','country').by(valueMap('name')).by(valueMap()).fold()).fold())" +
                "\"" +
                "}";
        String response = client.execute(queryString);                          // execute query
        ArrayList<JSONObject> resultList =
                gremlinResponseToResultArrayList(response);                     // convert response into result list
        ArrayList<LdbcQuery1Result> endResult                                   // init end result list
                = new ArrayList<>();
        for (JSONObject result : resultList) {                                  // for each result

            ArrayList<JSONObject> resultBreakdown = gremlinListToArrayList(result);             // result contains sub results
            HashMap<String,JSONObject> person = gremlinMapToHashMap(resultBreakdown.get(0));    // person result
            HashMap<String,JSONObject> city = gremlinMapToHashMap(resultBreakdown.get(1));      // city result
            int distanceFrom = Integer.parseInt(getPropertyValue(resultBreakdown.get(2)));      // distance result
            String emails = getPropertyValue(person.get("email"));                              // email result
            List<String> emailList =
                    Arrays.asList(
                            emails.replaceAll("[\\[\\]\\s+]", "").split(","));
            if (emailList.size() == 1 && emailList.get(0).equals("")) {
                emailList = new ArrayList<>();
            }
            String speaks = getPropertyValue(person.get("speaks"));                             // speaks result
            List<String> speaksList = Arrays.asList(
                    speaks.replaceAll("[\\[\\]\\s+]", "").split(","));

            ArrayList<JSONObject> universities = gremlinListToArrayList(resultBreakdown.get(4)); // university result
            ArrayList<List<Object>> universitiesResult = new ArrayList<>(); // universities
            if (universities.size() != 0) {
                for (JSONObject u : universities) { // foreach uni
                    HashMap<String, JSONObject> university = gremlinMapToHashMap(u);
                    ArrayList<Object> universityResult = new ArrayList<>();
                    Object countryName =
                            getPropertyValue(gremlinMapToHashMap(university.get("country")).get("name"));
                    Object universityName =
                            getPropertyValue(gremlinMapToHashMap(university.get("university")).get("name"));
                    Integer classYear =
                            Integer.parseInt(gremlinMapToHashMap(university.get("studyFrom")).get("classYear").get("@value").toString());
                    universityResult.add(universityName);
                    universityResult.add(classYear);
                    universityResult.add(countryName);
                    universitiesResult.add(universityResult);
                }
            }

            ArrayList<JSONObject> companies = gremlinListToArrayList(resultBreakdown.get(3));
            ArrayList<List<Object>> companiesResult = new ArrayList<>();

            if (companies.size() != 0) {
                for (JSONObject u : companies) {
                    HashMap<String, JSONObject> company = gremlinMapToHashMap(u);
                    ArrayList<Object> companyResult = new ArrayList<>();
                    Object countryName = getPropertyValue(gremlinMapToHashMap(company.get("country")).get("name"));
                    Object companyName = getPropertyValue(gremlinMapToHashMap(company.get("company")).get("name"));
                    Integer workFrom = Integer.parseInt(
                            gremlinMapToHashMap(company.get("workFrom")).get("workFrom").get("@value").toString());
                    companyResult.add(companyName);
                    companyResult.add(workFrom);
                    companyResult.add(countryName);
                    companiesResult.add(companyResult);
                }
            }

            LdbcQuery1Result res                                                    // create result object
                    = new LdbcQuery1Result(
                    Long.parseLong(getPropertyValue(person.get("id"))),             // personId
                    getPropertyValue(person.get("lastName")),                       // personLastName
                    distanceFrom,                                                   // distanceFrom
                    Long.parseLong(getPropertyValue(person.get("birthday"))),       // birthday
                    Long.parseLong(getPropertyValue(person.get("creationDate"))),   // creationDate
                    getPropertyValue(person.get("gender")),                         // gender
                    getPropertyValue(person.get("browserUsed")),                    // browser used
                    getPropertyValue(person.get("locationIP")),                     // location ip
                    emailList,                                                      // email list
                    speaksList,                                                     // speaks list
                    getPropertyValue(city.get("name")),                             // city name
                    universitiesResult,                                             // universities result
                    companiesResult                                                 // companies result
            );
            endResult.add(res);
        }
        resultReporter.report(0, endResult, operation);
    }
}
