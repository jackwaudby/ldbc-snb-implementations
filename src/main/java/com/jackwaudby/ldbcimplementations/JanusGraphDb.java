package com.jackwaudby.ldbcimplementations;

import com.jackwaudby.ldbcimplementations.queryhandlers.LdbcShortQuery1PersonProfileHandler;
import com.jackwaudby.ldbcimplementations.queryhandlers.LdbcShortQuery4MessageContentHandler;
import com.ldbc.driver.*;
import com.ldbc.driver.control.LoggingService;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery1PersonProfile;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery4MessageContent;
import org.apache.http.HttpClientConnection;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
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
import org.json.JSONObject;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;


public class JanusGraphDb extends Db {


    /**
     * Static nested class that creates a JanusGraph client
     */
    public static class JanusGraphClient {

        CloseableHttpClient httpClient;         // http client
        HttpClientContext context;              // http context
        HttpClientConnectionManager connMrg;    // http connection manager
        HttpRoute route;                        // http route
        HttpHost host;                          // http host
        ConnectionRequest connRequest;          // connection
        HttpClientConnection conn;              // client connection
        String connectionUrl;                   // connection url

        /**
         * JanusGraph client constructor
         * @param connectionUrl connection url of JanusGraph Server
         */
        private JanusGraphClient(String connectionUrl)  {

            this.connectionUrl = connectionUrl;
            httpClient = HttpClients.createDefault();               // create http client
            context = HttpClientContext.create();                   // create http context
            connMrg = new BasicHttpClientConnectionManager();       // create connection manager
            host = new HttpHost("localhost",8182);   // specify server host TODO: parse connection url
            route = new HttpRoute(host);                            // add host to route
            connRequest = connMrg.requestConnection(route, null);// request connection
            try {
                conn = connRequest.get(10, TimeUnit.SECONDS);         // obtain connection with 10 seconds
                if (!conn.isOpen()) {
                    connMrg.connect(conn, route, 1000, context);      // establish connection based on its route info
                    connMrg.routeComplete(conn, route, context);         // and mark it as route complete
                }
            } catch (InterruptedException | ExecutionException | IOException e) {
                e.printStackTrace();
            }
        }

        /**
         * Provides functionality to execute a query against JanusGraph
         * @return query result
         */
        public String execute(String queryString)  {

            String result = null;
            try {
                StringEntity params = new StringEntity(queryString);                    // create entity for request message
                HttpPost request = new HttpPost(URI.create("http://localhost:8182"));   // create http post request
                request.setEntity(params);                                              // add content entity to request
                CloseableHttpResponse response = httpClient.execute(request, context);   // execute request and get response
                HttpEntity message = response.getEntity();                          // get message
                String output = EntityUtils.toString(message);                      // convert to string
                JSONObject jo = new JSONObject(output);                             // convert to JSON
                result = jo.getJSONObject("result").getJSONObject("data").getJSONArray("@value").getString(0);
//                result = output;
                response.close();                                                   // close stream
            } catch (IOException ex) {
                ex.printStackTrace();
            }
            return result;
        }

        /**
         * Close client HTTP connection
         */
        void closeClient() {
            try {
                httpClient.close(); // close http connection
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Static nested class that implements DbConnectionState
     * Used to share any state that needs to be shared between OperationHandler instances.
     */
    public static class JanusGraphConnectionState extends DbConnectionState {

        private final JanusGraphClient janusGraphClient; // init. JanusGraph client

        /**
         * Creates a JanusGraph connection state
         * @param connectionUrl connection url to JanusGraph server
         */
        private JanusGraphConnectionState(String connectionUrl) {
            janusGraphClient = new JanusGraphClient(connectionUrl); // create JanusGraph client
        }

        /**
         * Returns the JanusGraph client
         * @return JanusGraph client
         */
        public JanusGraphClient getClient() {
            return janusGraphClient;
        }

        /**
         * Closes the JanusGraph client
         */
        @Override
        public void close()  {
            janusGraphClient.closeClient();
        }
    }

    private JanusGraphConnectionState connectionState = null; // init connection state

    /**
     * Get JanusGraph connection state
     * @return JanusGraph connection state
     * @throws DbException problem with SUT
     */
    @Override
    protected JanusGraphConnectionState getConnectionState() throws DbException {
        return connectionState;
    }

    /**
     * Called before the benchmark is run. Note, OperationHandler implementations must be registered here.
     * @param properties map of configuration properties
     * @throws DbException problem with SUT
     */
    @Override
    protected void onInit(Map<String, String> properties, LoggingService loggingService) throws DbException {

        String connectionUrl = properties.get("url"); // retrieve connection url
        connectionState = new JanusGraphConnectionState(connectionUrl); // create JanusGraph connection state

        // TODO: register operation handlers
        registerOperationHandler(LdbcShortQuery1PersonProfile.class, LdbcShortQuery1PersonProfileHandler.class);
        registerOperationHandler(LdbcShortQuery4MessageContent.class, LdbcShortQuery4MessageContentHandler.class);
    }


    /**
     * Called after benchmark has completed.
     */
    @Override
    protected void onClose() {
        connectionState.janusGraphClient.closeClient(); // perform clean up
    }

    // method not required by driver used for testing
    String execute(String queryString)  {
        return connectionState.getClient().execute(queryString);
    }

    // method not required by driver used for testing
    void init(Map<String, String> properties) {

        String connectionUrl = properties.get("url"); // retrieve connection url
        connectionState = new JanusGraphConnectionState(connectionUrl); // create JanusGraph connection state
    }


}
