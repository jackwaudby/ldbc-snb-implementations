package com.jackwaudby.ldbcimplementations.clients;

import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV1d0;


/**
 * Connecting to the Gremlin Server from a Java application using withRemote
 * This approach requires a Gremlin language driver the chosen programming language
 * Results from queries are serialized into Java variables
 */
public class RemoteClient {

    public static void main(String[] args) {

        // connect to the gremlin server
        Cluster.Builder builder = Cluster.build();
        builder.addContactPoint("localhost");
        builder.port(8182);
        builder.serializer(new GryoMessageSerializerV1d0());
        Cluster cluster = builder.create();

        // create graph traversal source
        GraphTraversalSource g = EmptyGraph.instance().traversal().withRemote(DriverRemoteConnection.using(cluster));

        // issue queries
        Long x = g.V().hasLabel("Person").count().next();
        System.out.println(x);

        // disconnect from gremlin server
        cluster.close();
    }

}