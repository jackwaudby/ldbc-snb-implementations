package com.jackwaudby.ldbcimplementations.clients;

import org.apache.http.*;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.conn.ConnectionRequest;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.BasicHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.json.JSONObject;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Connecting to the Gremlin Server via HTTP requests
 * Queries are packaged as HTTP POST requests and sends it to the Gremlin Server
 * The body of the HTTP request is encoded as JSON.
 */
public class HTTPClient {

    private static final Logger LOGGER = Logger.getLogger(HTTPClient.class);

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {

        // create http client
        CloseableHttpClient httpClient = HttpClients.createDefault();
        LOGGER.info("HTTP Client Created");
        // create http context
        HttpClientContext context = HttpClientContext.create();
        LOGGER.info("HTTP Context Created");
        // create a http connection manager
        HttpClientConnectionManager connMrg = new BasicHttpClientConnectionManager();
        LOGGER.info("HTTP Connection Manager Created");
        HttpRoute route = new HttpRoute(new HttpHost("localhost", 8182));
        // request new connection
        ConnectionRequest connRequest = connMrg.requestConnection(route, null);
        // Wait for connection up to 10 sec
        HttpClientConnection conn = connRequest.get(10, TimeUnit.SECONDS);
        try {
            // If not open
            if (!conn.isOpen()) {
                // establish connection based on its route info
                connMrg.connect(conn, route, 1000, context);
                // and mark it as route complete
                connMrg.routeComplete(conn, route, context);
                LOGGER.info("HTTP Connection Obtained");
            }
            // query string
            String queryString = "{\"gremlin\": \"def v = g.V().has('Person','id',5497558140022).next();[];def hm = g.V(v).valueMap('firstName','lastName','birthday','locationIP','browserUsed','gender','creationDate').next();[];def v2 = g.V(v).outE('isLocatedIn').inV().valueMap('id').next();[]; def cityId = v2['id'];[];hm.put('cityId',cityId);[];hm.toString()\"}";
            // create string entity
            StringEntity params = new StringEntity(queryString);
            // create request
            HttpPost request = new HttpPost("http://localhost:8182");
            // add content entity to request
            request.setEntity(params);
            // execute request and get response
            CloseableHttpResponse response = httpClient.execute(request,context);
            LOGGER.info("HTTP Request Sent");
            try {
                LOGGER.info("HTTP Response Received");
                LOGGER.info("Status Code: " + response.getStatusLine().getStatusCode());
                HttpEntity message = response.getEntity();
                // create JSON object
                JSONObject output = new JSONObject(EntityUtils.toString(message));
                // get result string

            } finally {
                response.close();
                LOGGER.info("HTTP Response Closed");
            }
        } finally {
            connMrg.releaseConnection(conn, null, 1, TimeUnit.MINUTES);
            LOGGER.info("Releasing Connection");
        }

//         shutdown the connection manager
        httpClient.close();
        LOGGER.info("Shutdown HTTP Client");

    }


}
