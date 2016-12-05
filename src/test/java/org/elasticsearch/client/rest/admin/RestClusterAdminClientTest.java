/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client.rest.admin;

import com.google.common.collect.Maps;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.hotthreads.NodesHotThreadsResponse;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryResponse;
import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesResponse;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryResponse;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteResponse;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.delete.DeleteSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotIndexStatus;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusRequest;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsResponse;
import org.elasticsearch.client.rest.AbstractRestClientTest;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.cluster.metadata.SnapshotMetaData;
import org.elasticsearch.cluster.routing.allocation.RoutingExplanations;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.RepositoryMissingException;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotMissingException;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;

/**
 */
public class RestClusterAdminClientTest extends AbstractRestClientTest {

    @Test
    public void testClusterStats() {
        ClusterStatsResponse response = clusterAdminClient.prepareClusterStats().get();
    }

    @Test
    public void testClusterState() {
        ClusterStateResponse response = clusterAdminClient.prepareState().get();
        ClusterState state = response.getState();
        assertNotNull(state);
        assertNotNull(state.getMetaData());
        ImmutableOpenMap<String, IndexMetaData> indices = state.getMetaData().indices();
        assertNotNull(indices);
        assertTrue(indices.size() > 0);
        assertNotNull(state.routingTable());
        assertNotNull(state.blocks());
        ClusterName clusterName = response.getClusterName();
        assertNotNull(clusterName);
        assertNotNull(clusterName.value());
    }

    @Test
    public void testHealth() {
        ClusterHealthResponse response = clusterAdminClient.prepareHealth().get();
        assertNotNull(response.getClusterName());
    }

    @Test
    public void testCrudRepositories() {
        String repoName = "repo-" + UUID.randomUUID().toString();
        Map<String, Object> settings = Maps.newLinkedHashMap();
        settings.put("location", "/tmp/" + repoName);
        PutRepositoryResponse putResponse = clusterAdminClient.preparePutRepository(repoName)
                .setType("fs")
                .setSettings(settings)
                .get();
        assertAcknowledged(putResponse);

        GetRepositoriesResponse getResponse = clusterAdminClient.prepareGetRepositories(repoName).get();
        Iterator<RepositoryMetaData> metaDataIterator = getResponse.iterator();
        assertTrue(metaDataIterator.hasNext());
        RepositoryMetaData metaData = metaDataIterator.next();
        assertEquals(repoName, metaData.name());

        DeleteRepositoryResponse deleteResponse = clusterAdminClient.prepareDeleteRepository(repoName).get();
        assertAcknowledged(deleteResponse);
    }

    @Test
    public void testCrudSnapshotsWaitForCompletion() throws InterruptedException, ExecutionException {
        indexDocument(1000);
        String repoName = "repo-" + UUID.randomUUID().toString();
        Map<String, Object> settings = Maps.newLinkedHashMap();
        settings.put("location", "/tmp/" + repoName);
        PutRepositoryResponse putResponse = clusterAdminClient.preparePutRepository(repoName)
                .setType("fs")
                .setSettings(settings)
                .get();
        assertAcknowledged(putResponse);

        String snapshotName = "snapshot-" + UUID.randomUUID().toString();
        CreateSnapshotResponse snapshotResponse;
        snapshotResponse = clusterAdminClient.prepareCreateSnapshot(repoName, snapshotName)
                .setIndices(index)
                .setWaitForCompletion(true)
                .get();
        SnapshotInfo snapshotInfo = snapshotResponse.getSnapshotInfo();
        assertNotNull(snapshotInfo);
        assertEquals(snapshotName, snapshotInfo.name());
        assertFalse(snapshotInfo.indices().isEmpty());

        DeleteSnapshotResponse deleteSnapshotResponse = clusterAdminClient.prepareDeleteSnapshot(repoName, snapshotName).get();
        assertAcknowledged(deleteSnapshotResponse);
    }

    @Test(timeout = 20000)
    public void testCrudSnapshotsAsync() throws InterruptedException, ExecutionException {
        indexDocument(1000);
        String repoName = "repo-" + UUID.randomUUID().toString();
        Map<String, Object> settings = Maps.newLinkedHashMap();
        settings.put("location", "/tmp/" + repoName);
        PutRepositoryResponse putResponse = clusterAdminClient.preparePutRepository(repoName)
                .setType("fs")
                .setSettings(settings)
                .get();
        assertAcknowledged(putResponse);

        String snapshotName = "snapshot-" + UUID.randomUUID().toString();
        CreateSnapshotResponse snapshotResponse;
        snapshotResponse = clusterAdminClient.prepareCreateSnapshot(repoName, snapshotName)
                .setIndices(index)
                .setWaitForCompletion(false)
                .get();

        SnapshotsStatusResponse snapshotsStatusResponse;
        snapshotsStatusResponse = clusterAdminClient.snapshotsStatus(new SnapshotsStatusRequest(repoName)).actionGet();
        assertEquals(1, snapshotsStatusResponse.getSnapshots().size());
        Map<String, SnapshotIndexStatus> indices = snapshotsStatusResponse.getSnapshots().get(0).getIndices();
        assertEquals(1, indices.size());
        SnapshotIndexStatus snapshotIndexShardStatuses = indices.get(index);
        assertNotNull(snapshotIndexShardStatuses);

        for (;;) {
            SnapshotsStatusResponse response = clusterAdminClient.snapshotsStatus(new SnapshotsStatusRequest(repoName)).actionGet();
            if (response.getSnapshots().isEmpty()) {
                break;
            }
            if (response.getSnapshots().get(0).getState() == SnapshotMetaData.State.SUCCESS) {
                break;
            }
        }

        DeleteSnapshotResponse deleteSnapshotResponse = clusterAdminClient.prepareDeleteSnapshot(repoName, snapshotName).get();
        assertAcknowledged(deleteSnapshotResponse);
    }

    @Test(expected = RepositoryMissingException.class)
    public void testRepositoryMissingException() {
        clusterAdminClient.prepareGetRepositories(UUID.randomUUID().toString()).get();
    }

    @Test(expected = SnapshotMissingException.class)
    public void testSnapshotMissingException() {
        String repoName = "repo-" + UUID.randomUUID().toString();
        Map<String, Object> settings = Maps.newLinkedHashMap();
        settings.put("location", "/tmp/" + repoName);
        PutRepositoryResponse putResponse = clusterAdminClient.preparePutRepository(repoName)
                .setType("fs")
                .setSettings(settings)
                .get();
        assertAcknowledged(putResponse);

        clusterAdminClient.prepareGetSnapshots(repoName).addSnapshots(UUID.randomUUID().toString()).get();
    }

    @Test
    @Ignore
    public void testNodesHotThreads() {
        NodesHotThreadsResponse response = clusterAdminClient.prepareNodesHotThreads().get();
        // Currently broke, NodesHotThreadsResponse needs to return proper json.
    }

    /**
     * This looks deprecated in later releases
     */
    @Test
    @Ignore
    public void testNodesRestart() {
        clusterAdminClient.prepareNodesRestart().get();
    }

    @Test
    public void testReroute() {
        ClusterRerouteResponse response = clusterAdminClient.prepareReroute()
                .setDryRun(true)
                .setExplain(true)
                .get();
        assertNotNull(response);
        //todo setup multi-node test
    }

    @Test
    public void testNodesInfo() {
        NodesInfoResponse response = clusterAdminClient.prepareNodesInfo().all().get();
        assertNotNull(response);
    }

}
