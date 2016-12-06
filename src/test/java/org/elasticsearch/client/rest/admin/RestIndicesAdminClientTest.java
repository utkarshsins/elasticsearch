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

import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.alias.exists.AliasesExistRequestBuilder;
import org.elasticsearch.action.admin.indices.alias.exists.AliasesExistResponse;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheResponse;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.delete.DeleteMappingResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateResponse;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateResponse;
import org.elasticsearch.client.rest.AbstractRestClientTest;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.FilterBuilders;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.*;

/**
 */
public class RestIndicesAdminClientTest extends AbstractRestClientTest {



    @Test
    public void testGetIndex() {
        GetIndexResponse response;
        response = indicesAdminClient.prepareGetIndex().addIndices(index).get();
        assertFalse(response.getMappings().get(index).isEmpty());
        assertFalse(response.getSettings().get(index).names().isEmpty());
        assertFalse(response.getAliases().get(index).isEmpty());
    }



    @Test
    public void testFlushIndex() {
        FlushResponse response = indicesAdminClient.prepareFlush(index).get();
        assertTrue(response.getFailedShards() == 0);
        assertTrue(response.getSuccessfulShards() > 0);
    }

    @Test
    public void testCloseIndex() {
        closeIndex();
    }

    @Test
    public void testOpenIndex() throws InterruptedException {
        closeIndex();
        Thread.sleep(2000);

        OpenIndexResponse response = indicesAdminClient.prepareOpen(index).get();
        assertAcknowledged(response);
    }

    @Test
    public void testExistsIndex() {
        IndicesExistsResponse response = indicesAdminClient.prepareExists(index).get();
        assertTrue(response.isExists());
    }

    @Test
    public void testPutTemplate() {
        putTemplate();
    }

    private void putTemplate() {
        PutIndexTemplateResponse response;
        response = indicesAdminClient.preparePutTemplate("logs_template").setSource(loadTestIndexTemplate()).get();
        assertAcknowledged(response);
    }

    @Test
    public void testDeleteTemplate() {
        putTemplate();

        DeleteIndexTemplateResponse response;
        response = indicesAdminClient.prepareDeleteTemplate("logs_template").get();
        assertAcknowledged(response);
    }

    @Test
    public void testAliases() {
        IndicesAliasesResponse response;
        response = indicesAdminClient.prepareAliases()
                .addAlias(index, "myAliasNoArg")
                .addAlias(index, "blueGuy", FilterBuilders.termFilter("color", "blue"))
                .removeAlias(index, "alias_1").get();
        assertAcknowledged(response);
    }

    @Test
    public void testClearCache() {
        ClearIndicesCacheResponse response = indicesAdminClient.prepareClearCache(index).get();
        assertBroadcastOperationResponse(response);
    }

    @Test
    public void testDeleteMapping() {
        DeleteMappingResponse response = indicesAdminClient.prepareDeleteMapping(index).setType(type).get();
        assertAcknowledged(response);
    }

    @Test
    public void testGetMapping() throws IOException {
        GetMappingsResponse response = indicesAdminClient.prepareGetMappings(index).get();
        ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings;
        mappings = response.getMappings();
        assertTrue(!mappings.isEmpty());
        ImmutableOpenMap<String, MappingMetaData> mapping = mappings.get(index);
        assertNotNull(mapping);
        assertTrue(!mapping.isEmpty());
        MappingMetaData stats = mapping.get("stats");
        assertNotNull(stats);
    }

    @Test
    public void testPutMapping() throws IOException {
        String mapping = loadTestIndexPutMapping();
        PutMappingResponse response = indicesAdminClient.preparePutMapping(index)
                .setType("stats")
                .setSource(mapping)
                .setIgnoreConflicts(true).get();
        assertAcknowledged(response);
    }

    @Test
    public void testRefreshRequest() {
        RefreshResponse response = indicesAdminClient.prepareRefresh(index).get();
        assertBroadcastOperationResponse(response);
    }

    @Test
    public void testGetSettings() {
        GetSettingsResponse response = indicesAdminClient.prepareGetSettings(index).get();
        ImmutableOpenMap<String, Settings> indexToSettings = response.getIndexToSettings();
        assertFalse(indexToSettings.isEmpty());
        Settings settings = indexToSettings.get(index);
        assertNotNull(settings);
        assertNotNull(settings.get("index.creation_date"));
        assertEquals(1, settings.getAsInt("index.number_of_shards", -1).intValue());
        assertEquals(0, settings.getAsInt("index.number_of_replicas", -1).intValue());
    }

    @Test
    public void testGetIndexTemplates() {
        PutIndexTemplateResponse putIndexTemplateResponse = indicesAdminClient.preparePutTemplate("logs_template").setSource(loadTestIndexTemplate()).get();
        assertAcknowledged(putIndexTemplateResponse);
        GetIndexTemplatesResponse response = indicesAdminClient.prepareGetTemplates().get();
        List<IndexTemplateMetaData> indexTemplates = response.getIndexTemplates();
        assertNotNull(indexTemplates);

        assertTrue(indexTemplates.size() > 0);
        IndexTemplateMetaData indexTemplateMetaData = indexTemplates.get(0);
        assertEquals("logs_template", indexTemplateMetaData.getName());
        assertEquals("logs_*", indexTemplateMetaData.getTemplate());
        assertEquals(0, indexTemplateMetaData.getOrder());
    }

    @Test
    public void getAliases() {
        GetAliasesResponse response = indicesAdminClient.prepareGetAliases().get();
        ImmutableOpenMap<String, List<AliasMetaData>> aliases = response.getAliases();
        assertNotNull(aliases);
        assertFalse(aliases.isEmpty());

        List<AliasMetaData> meta = aliases.get(index);
        assertNotNull(meta);
        assertTrue(meta.size() > 0);
        assertEquals("alias_1", meta.get(0).alias());
    }

    @Test
    public void testAliasesExist() {

        AliasesExistResponse response;
        AliasesExistRequestBuilder builder = indicesAdminClient.prepareAliasesExist("alias_1");
        response = builder.get();
        assertFalse(response.exists());

        response = indicesAdminClient.prepareAliasesExist(UUID.randomUUID().toString()).get();
        assertFalse(response.exists());

    }

    private void closeIndex() {
        CloseIndexResponse response = indicesAdminClient.prepareClose(index).get();
        assertAcknowledged(response);
    }


}
