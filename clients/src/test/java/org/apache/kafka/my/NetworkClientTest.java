/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.my;

import org.apache.kafka.clients.*;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.network.NetworkReceive;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.test.DelayedReceive;
import org.apache.kafka.test.MockSelector;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

public class NetworkClientTest {

    protected final int minRequestTimeoutMs = 1000;
    protected final MockTime time = new MockTime();
    protected final MockSelector selector = new MockSelector(time);
    protected final Metadata metadata = new Metadata(0, Long.MAX_VALUE, true);
    protected final int nodeId = 1;
    protected final Cluster cluster = TestUtils.singletonCluster("test", nodeId);
    protected final Node node = cluster.nodes().get(0);
    protected final long reconnectBackoffMsTest = 10 * 1000;
    protected final long reconnectBackoffMaxMsTest = 10 * 10000;

    private final NetworkClient client = createNetworkClient(reconnectBackoffMaxMsTest);

    private NetworkClient createNetworkClient(long reconnectBackoffMaxMs) {
        return new NetworkClient(selector, metadata, "mock", Integer.MAX_VALUE,
                reconnectBackoffMsTest, reconnectBackoffMaxMs, 64 * 1024, 64 * 1024,
                minRequestTimeoutMs, time, true, new ApiVersions(), new LogContext());
    }

    @Before
    public void setup() {
        selector.reset();
        metadata.update(cluster, Collections.<String>emptySet(), time.milliseconds());
    }

    @Test
    public void testRequestTimeout() {
        awaitReady(client, node); // has to be before creating any request, as it may send ApiVersionsRequest and its response is mocked with correlation id 0
        ProduceRequest.Builder builder = ProduceRequest.Builder.forCurrentMagic((short) 1, 1000, Collections.emptyMap());
        int requestTimeoutMs = minRequestTimeoutMs + 5000;
        ClientRequest request = client.newClientRequest(node.idString(), builder, time.milliseconds(), true,
                requestTimeoutMs, new RequestCompletionHandler() {
                    @Override
                    public void onComplete(ClientResponse response) {
                        System.out.println("Request completed!");
                    }
                });
        testRequestTimeout(request);
    }

    @Test
    public void testLeastLoadedNode() {
        client.ready(node, time.milliseconds());
        awaitReady(client, node);
        client.poll(1, time.milliseconds());
        assertTrue("The client should be ready", client.isReady(node, time.milliseconds()));

        // leastloadednode should be our single node
        Node leastNode = client.leastLoadedNode(time.milliseconds());
        assertEquals("There should be one leastloadednode", leastNode.id(), node.id());

        // sleep for longer than reconnect backoff
        time.sleep(reconnectBackoffMsTest);

        // CLOSE node
        selector.serverDisconnect(node.idString());

        client.poll(1, time.milliseconds());
        assertFalse("After we forced the disconnection the client is no longer ready.", client.ready(node, time.milliseconds()));
        leastNode = client.leastLoadedNode(time.milliseconds());
        assertNull("There should be NO leastloadednode", leastNode);
    }


    private void setExpectedApiVersionsResponse(ApiVersionsResponse response) {
        short apiVersionsResponseVersion = response.apiVersion(ApiKeys.API_VERSIONS.id).maxVersion;
        ByteBuffer buffer = response.serialize(apiVersionsResponseVersion, new ResponseHeader(0));
        selector.delayedReceive(new DelayedReceive(node.idString(), new NetworkReceive(node.idString(), buffer)));
    }

    private void awaitReady(NetworkClient client, Node node) {
        if (client.discoverBrokerVersions()) {
            setExpectedApiVersionsResponse(ApiVersionsResponse.defaultApiVersionsResponse());
        }
        while (!client.ready(node, time.milliseconds()))
            client.poll(1, time.milliseconds());
        selector.clear();
    }

    private void testRequestTimeout(ClientRequest request) {
        client.send(request, time.milliseconds());

        time.sleep(request.requestTimeoutMs() + 1);
        List<ClientResponse> responses = client.poll(0, time.milliseconds());

        assertEquals(1, responses.size());
        ClientResponse clientResponse = responses.get(0);
        assertEquals(node.idString(), clientResponse.destination());
        assertTrue("Expected response to fail due to disconnection", clientResponse.wasDisconnected());
    }
}
