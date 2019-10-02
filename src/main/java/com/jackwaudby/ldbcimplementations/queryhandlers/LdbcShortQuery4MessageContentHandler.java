//package com.jackwaudby.ldbcimplementations.queryhandlers;
//
//import com.jackwaudby.ldbcimplementations.JanusGraphDb;
//import static com.jackwaudby.ldbcimplementations.utils.ParseDateLong.creationDateStringToLong;
//import com.jackwaudby.ldbcimplementations.utils.HttpResponseToResultMap;
//import com.ldbc.driver.DbException;
//import com.ldbc.driver.OperationHandler;
//import com.ldbc.driver.ResultReporter;
//import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery4MessageContent;
//import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery4MessageContentResult;
//import org.json.JSONObject;
//
//import java.text.ParseException;
//import java.util.HashMap;
//
///**
// * Given a Message, retrieve its content and creation date.
// */
//public class LdbcShortQuery4MessageContentHandler implements OperationHandler<LdbcShortQuery4MessageContent,JanusGraphDb.JanusGraphConnectionState> {
//
//    //TODO: Add transaction and retry logic
//
//    @Override
//    public void executeOperation(LdbcShortQuery4MessageContent operation, JanusGraphDb.JanusGraphConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
//
//        long messageId = operation.messageId();
//        // get JanusGraph client
//        JanusGraphDb.JanusGraphClient client = dbConnectionState.getClient();
//        // gremlin query string
//        String queryString = "{\"gremlin\": \"post_exists = g.V().has('Post','id'," + messageId + ").hasNext();[];if(post_exists){v=g.V().has('Post','id'," + messageId + ").valueMap('creationDate','content','imageFile').next();[]} else {v=g.V().has('Comment','id'," + messageId + ").valueMap('creationDate','content','imageFile').next();[]};v.toString()\"}";
//        try {
//            String result = client.execute(queryString);
//            JSONObject jo = new JSONObject(result);                             // convert to JSON
//            result = jo.getJSONObject("result").getJSONObject("data").getJSONArray("@value").getString(0);
//            HashMap<String, String> resultMap = HttpResponseToResultMap.resultToMap(result);
//            LdbcShortQuery4MessageContentResult endResult;
//            if (resultMap.get("content").isEmpty()) {
//                        endResult = new LdbcShortQuery4MessageContentResult(
//                        resultMap.get("imageFile"),
//                        creationDateStringToLong(resultMap.get("creationDate")));
//            } else {
//                        endResult = new LdbcShortQuery4MessageContentResult(
//                        resultMap.get("content"),
//                        creationDateStringToLong(resultMap.get("creationDate")));
//            }
//            // pass result to driver
//            resultReporter.report(0, endResult, operation);
//        } catch (ParseException e) {
//            e.printStackTrace();
//        }
//    }
//}
