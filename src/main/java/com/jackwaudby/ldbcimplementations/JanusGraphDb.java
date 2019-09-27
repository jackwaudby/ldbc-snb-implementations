package com.jackwaudby.ldbcimplementations;

import com.jackwaudby.ldbcimplementations.queryhandlers.LdbcShortQuery1PersonProfileHandler;
import com.ldbc.driver.*;
import com.ldbc.driver.control.LoggingService;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery1PersonProfile;
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
import org.json.JSONException;
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
         * @throws InterruptedException
         * @throws ExecutionException
         * @throws IOException
         */
        public JanusGraphClient(String connectionUrl) throws InterruptedException, ExecutionException, IOException {

            this.connectionUrl = connectionUrl;
            httpClient = HttpClients.createDefault();               // create http client
            context = HttpClientContext.create();                   // create http context
            connMrg = new BasicHttpClientConnectionManager();       // create connection manager
            host = new HttpHost("localhost",8182);   // specify server host TODO: parse connection url
            route = new HttpRoute(host);                            // add host to route
            connRequest = connMrg.requestConnection(route, null);// request connection
            conn = connRequest.get(10, TimeUnit.SECONDS);         // obtain connection with 10 seconds
            if (!conn.isOpen()) {
                connMrg.connect(conn, route, 1000, context);      // establish connection based on its route info
                connMrg.routeComplete(conn, route, context);         // and mark it as route complete
            }
        }

        /**
         * Provides functionality to execute a query against JanusGraph
         * @return
         */
        public String execute(String queryString) throws IOException {

            StringEntity params = new StringEntity(queryString);                    // create entity for request message
            HttpPost request = new HttpPost(URI.create("http://localhost:8182"));   // create http post request
            request.setEntity(params);                                              // add content entity to request
            CloseableHttpResponse response = httpClient.execute(request,context);   // execute request and get response
            String result = null;
            try {
                HttpEntity message = response.getEntity();                          // get message
                String output = EntityUtils.toString(message);                      // convert to string
                JSONObject jo = new JSONObject(output);                             // convert to JSON
                try {
                    result = jo.getJSONObject("result").getJSONObject("data").getJSONArray("@value").getString(0);
                } catch (JSONException e) {
                    System.out.println(e);
                }
            } finally {
                response.close(); // close stream

            }
            return result;
        }

        /**
         * Close client HTTP connection
         * @throws IOException
         */
        void closeClient() throws IOException {
            httpClient.close(); // close http connection
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
         * @throws InterruptedException
         * @throws ExecutionException
         * @throws IOException
         */
        private JanusGraphConnectionState(String connectionUrl) throws InterruptedException, ExecutionException, IOException {
            janusGraphClient = new JanusGraphClient(connectionUrl); // create JanusGraph client
        }

        /**
         * Returns the JanusGraph client
         * @return
         */
        public JanusGraphClient getClient() {
            return janusGraphClient;
        }

        /**
         * Closes the JanusGraph client
         * @throws IOException
         */
        @Override
        public void close() throws IOException {
            janusGraphClient.closeClient();
        }
    }

    private JanusGraphConnectionState connectionState = null; // init connection state

    /**
     * Get JanusGraph connection state
     * @return JanusGraph connection state
     * @throws DbException
     */
    @Override
    protected JanusGraphConnectionState getConnectionState() throws DbException {
        return connectionState;
    }

    /**
     * Called before the benchmark is run. Note, OperationHandler implementations must be registered here.
     * @param properties map of configuration properties
     * @throws DbException
     */
    @Override
    protected void onInit(Map<String, String> properties, LoggingService loggingService) throws DbException {

        String connectionUrl = properties.get("url"); // retrieve connection url
        try {
            connectionState = new JanusGraphConnectionState(connectionUrl); // create JanusGraph connection state
        } catch (InterruptedException | ExecutionException | IOException e) {
            e.printStackTrace();
        }

        // TODO: register operation handlers
        registerOperationHandler(LdbcShortQuery1PersonProfile.class, LdbcShortQuery1PersonProfileHandler.class);

    }

    /**
     * Called after benchmark has completed.
     * @throws IOException
     */
    @Override
    protected void onClose() throws IOException {
        connectionState.janusGraphClient.closeClient(); // perform clean up
    }

    // method not required by driver used for testing
    public String execute(String queryString) throws IOException {
        String x = connectionState.getClient().execute(queryString);
        return x;
    }

    // method not required by driver used for testing
    void init(Map<String, String> properties) {

        String connectionUrl = properties.get("url"); // retrieve connection url
        try {
            connectionState = new JanusGraphConnectionState(connectionUrl); // create JanusGraph connection state
        } catch (InterruptedException | ExecutionException | IOException e) {
            e.printStackTrace();
        }
    }


}
