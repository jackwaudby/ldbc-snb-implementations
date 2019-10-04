package com.jackwaudby.ldbcimplementations;

import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate1AddPerson;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import static com.jackwaudby.ldbcimplementations.utils.HttpResponseToResultMap.httpResponseToResultMap;

public class QueryTestBed {
    public static void main(String[] args)  {

        // 434
        // TODO: add another property "language" for post
        // TODO: change person's "languages" to speaks
        // TODO: make changes in Query 1

        List<Long> tagIds = new ArrayList<>();
        tagIds.add(30L);
        tagIds.add(60L);
        tagIds.add(80L);

        LdbcUpdate1AddPerson.Organization organization1 = new LdbcUpdate1AddPerson.Organization(100,2010);
        LdbcUpdate1AddPerson.Organization organization2 = new LdbcUpdate1AddPerson.Organization(101,2019);
        LdbcUpdate1AddPerson.Organization organization3 = new LdbcUpdate1AddPerson.Organization(102,2017);
        LdbcUpdate1AddPerson.Organization organization4 = new LdbcUpdate1AddPerson.Organization(200,1910);
        LdbcUpdate1AddPerson.Organization organization5 = new LdbcUpdate1AddPerson.Organization(201,1919);
        LdbcUpdate1AddPerson.Organization organization6 = new LdbcUpdate1AddPerson.Organization(202,1917);


        List<LdbcUpdate1AddPerson.Organization> organizationList = new ArrayList<>();
        organizationList.add(organization1);
        organizationList.add(organization2);
        organizationList.add(organization3);

        List<LdbcUpdate1AddPerson.Organization> organizationList2 = new ArrayList<>();
        organizationList2.add(organization4);
        organizationList2.add(organization5);
        organizationList2.add(organization6);

        List<Long> companyIds = new ArrayList<>();
        List<Integer> workFrom = new ArrayList<>();
        for (int i = 0; i < organizationList.size(); i++) {
            companyIds.add(i,organizationList.get(i).organizationId());
            workFrom.add(i,organizationList.get(i).year());
        }
        System.out.println(companyIds.toString());
        System.out.println(workFrom.toString());
        System.out.println(tagIds.toString());

        List<Long> uniIds = new ArrayList<>();
        List<Integer> classYear = new ArrayList<>();
        for (int i = 0; i < organizationList2.size(); i++) {
            uniIds.add(i,organizationList2.get(i).organizationId());
            classYear.add(i,organizationList2.get(i).year());
        }
        System.out.println(uniIds.toString());
        System.out.println(classYear.toString());



//        printOperation("Update1AddPerson");

        //        Query 1
        //        valid: 1099511630063
        //        not valid:  68

        String readQuery1 = "{\"gremlin\": \"" +
                "try { " +
                "v = g.V().has('Person','id',5497558139615).next();[];" +
                "hm = g.V(v).valueMap('id','firstName','lastName','birthday','locationIP','browserUsed','gender','creationDate','language','email').next();[];" +
                "v2 = g.V(v).outE('isLocatedIn').inV().valueMap('id').next();[];" +
                "cityId = v2['id'];[];" +
                "hm.put('cityId',cityId);[];" +
                "graph.tx().commit();[];" +
                "hm;" +
                "} catch (Exception e) {" +
                "errorMessage =[e.toString()];[];" +
                "hm=[query_error:errorMessage];[];" +
                "graph.tx().rollback();[];" +
                "};" +
                "hm;\"" +
                "}";

        //        Query 4
        String readQuery4 =  "{\"gremlin\": \"post_exists = g.V().has('Post','id',42949783512).hasNext();[];" +
                "if(post_exists){" +
                "v=g.V().has('Post','id',42949783512).valueMap('creationDate','content','imageFile').next();[]" +
                "} else {" +
                "v=g.V().has('Comment','id',42949783512).valueMap('creationDate','content','imageFile').next();[]" +
                "};" +
                "v\"}";

        String query4 = "{\"gremlin\": \"" +
                "try {" +
                "post_exists = g.V().has('Post','id',42949673079).hasNext();[];" +
                "if(post_exists){" +
                "v=g.V().has('Post','id',42949673079).valueMap('creationDate','content','imageFile').next();[];" +
                "} else {" +
                "v=g.V().has('Comment','id',42949673079).valueMap('creationDate','content','imageFile').next();[];" +
                "};" +
                "graph.tx().commit();[];" +
                "} catch (Exception e) {" +
                "errorMessage =[e.toString()];[];" +
                "v=[query_error:errorMessage];[];" +
                "graph.tx().rollback();[];" +
                "};" +
                "v;\"" +
                "}";

//        Update - this works!
        String update1 = "{\"gremlin\": \"" +
                "try {" +
                "p = g.addV('Person').property('id',74).property('firstName','John').property('lastName','Doe')" +
                ".property('gender','female').property('birthday',789999).property('creationDate',789999)" +
                ".property('locationIP','56,89.567.445').property('browserUsed','Safari').next();[];" +
                "g.V().has('Place', 'id', 526).as('city').V(p).addE('isLocatedIn').to('city').next();[];" +
                "languages=['en','de'];[];"+
                "for (item in languages) { " +
                " g.V(p).property(set, 'language', item).next();[];" +
                "}; "+
                "email=['jack726@hotmail.com','jack726@icloud.com'];[];"+
                "for (item in email) { " +
                " g.V(p).property(set, 'email', item).next();[];" +
                "}; "+
                "tagid=" +
                tagIds.toString() +
                ";[];"+
                "for (item in tagid) { " +
                "g.V().has('Tag', 'id', item).as('tag').V(p).addE('hasInterest').to('tag').next();[];" +
                "};" +
                "companyId=" +
                companyIds.toString() +
                ";[];" +
                "workFrom=" +
                workFrom.toString() +
                ";[];" +
                "for (i = 0; i < companyId.size();i++){" +
                "g.V().has('Organisation', 'id', companyId[i]).as('comp').V(p).addE('workAt').property('workFrom',workFrom[i]).to('comp').next();[];" +
                "};" +
                "uniId=" +
                uniIds.toString() +
                ";[];" +
                "classYear=" +
                classYear.toString() +
                ";[];" +
                "for (i = 0; i < uniId.size();i++){" +
                "g.V().has('Organisation', 'id', uniId[i]).as('uni').V(p).addE('studyAt').property('classYear',classYear[i]).to('uni').next();[];" +
                "};" +
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

        String delete  = "{\"gremlin\": \"g.V().has('Person','id',5497558139615).drop()\"}";

        // 2010-04-19T04:21:24.931+0000

        String edgeLookup = "{\"gremlin\": \"" +
                "g.V().has('Person','id',1063).outE('knows').as('e').inV().has('Person','id',1959).select('e').values('creationDate')" +
                "\"" +
                "}";


        String postAdd = "{\"gremlin\": \"try {" +
                "p = g.addV('Post')" +
                ".property('id'," + 42949847055L + ")" +
                ".property('imageFile','" + "photo42949847055.jpg" + "')" +
                ".property('creationDate','" + 92183129038L  + "')" +
                ".property('locationIP','" + "31.128.15.41"  + "')" +
                ".property('browserUsed','" + "safari"  + "')" +
                ".property('language','" + "en"  + "')" +
                ".property('length','"+ 10 + "').next();[];" +
                "g.V().has('Person', 'id',"+ 1099511630063L +").as('person').V(p).addE('hasCreator').to('person').next();[];" +
                "g.V().has('Place', 'id',"+ 546 +").as('country').V(p).addE('isLocatedIn').to('country').next();[];" +
                "g.V(p).as('forum').V().has('Forum','id'," +
                4 +
                ").addE('containerOf').to('forum').next();[];" +
                "tagid=" +
                "[]" +
                ";[];"+
                "for (item in tagid) { " +
                "g.V().has('Tag', 'id', item).as('tag').V(p).addE('hasTag').to('tag').next();[];" +
                "};" +
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

        String postRead = "{\"gremlin\": \"" +
                "g.V().has('Post','id',42949847055).valueMap()" +
                "\"" +
                "}";

        Map< String,String> hm = new HashMap<>();                               // init test bed
        hm.put("url","http://localhost:8182");
        JanusGraphDb x = new JanusGraphDb();
        x.init(hm);

        int TX_ATTEMPTS = 0;
        int TX_RETRIES = 5;
        while (TX_ATTEMPTS < TX_RETRIES) {
            System.out.println("Attempt " + (TX_ATTEMPTS + 1));
            String response = x.execute(postRead);                                // get response as string
            System.out.println(response);
            HashMap<String, String> result = httpResponseToResultMap(response);      // convert to result map
            if (result.containsKey("query_error")) {
                TX_ATTEMPTS = TX_ATTEMPTS + 1;
                System.out.println("Query Error: " + result.get("query_error"));
            } else if (result.containsKey("http_error")) {
                TX_ATTEMPTS = TX_ATTEMPTS + 1;
                System.out.println("Gremlin Server Error: " + result.get("http_error"));
            } else {
                // create result object
                System.out.println(result.toString());
                break;
            }
        }


//        x.onClose();                                                            // close test bed

    }


    public static String extractData(String httpResponse) {

        JSONObject jsonObject = new JSONObject(httpResponse);
        String result=null;
        try {
            result = jsonObject.getJSONObject("result").getJSONObject("data").getJSONArray("@value").getString(0);
        } catch (JSONException e){
            e.printStackTrace();
        }
        return result;
    }

    static void printOperation(String operationName) {
        String pathToCsv = "/Users/jackwaudby/Documents/janusgraph/validation/validation_params_subset.csv";
        try {
            BufferedReader csvReader = new BufferedReader(new FileReader(pathToCsv));
            String row;
            while ((row = csvReader.readLine()) != null) {
                String[] aRow = row.split("\\|"); // splits into query | result
                String[] typeParam = aRow[0].split(","); // splits query on ,
                String operationType = typeParam[0].replaceAll("\"", "") // gets query type
                        .replace("[com.ldbc.driver.workloads.ldbc.snb.interactive.Ldbc", "");
                ArrayList<String> operationParams = new ArrayList<>();
                for (int i = 1; i < typeParam.length; i++) { // gets query params
                    operationParams.add(typeParam[i].replaceAll("\",", ""));
                }
                String[] queryResult = aRow[1].replaceAll("\",", "|").replaceAll(",\"", "|").replaceAll("[\\[\\]\"]", "").split("\\|");


                if (operationType.contains(operationName)) {
                    System.out.println("Query Type: " + operationType);
                    System.out.println("Query Parameters: " + operationParams);
                    System.out.println("Query Result: " + Arrays.toString(queryResult));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
