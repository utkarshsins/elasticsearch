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
package org.elasticsearch.client.rest.support;

import com.google.common.collect.Maps;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.util.concurrent.UncategorizedExecutionException;
import org.elasticsearch.common.xcontent.XContentObject;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.indices.IndexMissingException;

import java.util.Locale;
import java.util.Map;

/**
 *
 */
public enum ElasticsearchExceptionHandler {
    INDEX_SHARD_SNAPSHOT_FAILED_EXCEPTION(org.elasticsearch.index.snapshots.IndexShardSnapshotFailedException.class
    ),
    DFS_PHASE_EXECUTION_EXCEPTION(org.elasticsearch.search.dfs.DfsPhaseExecutionException.class
    ),
    MASTER_NOT_DISCOVERED_EXCEPTION(org.elasticsearch.discovery.MasterNotDiscoveredException.class
    ),
    INDEX_SHARD_RESTORE_EXCEPTION(org.elasticsearch.index.snapshots.IndexShardRestoreException.class
    ),
    INDEX_CLOSED_EXCEPTION(org.elasticsearch.indices.IndexClosedException.class
    ),
    BIND_HTTP_EXCEPTION(org.elasticsearch.http.BindHttpException.class
    ),
    REDUCE_SEARCH_PHASE_EXCEPTION(org.elasticsearch.action.search.ReduceSearchPhaseException.class
    ),
    NODE_CLOSED_EXCEPTION(org.elasticsearch.node.NodeClosedException.class
    ),
    SNAPSHOT_FAILED_ENGINE_EXCEPTION(org.elasticsearch.index.engine.SnapshotFailedEngineException.class
    ),
    SHARD_NOT_FOUND_EXCEPTION(org.elasticsearch.index.shard.ShardNotFoundException.class
    ),
    CONNECT_TRANSPORT_EXCEPTION(org.elasticsearch.transport.ConnectTransportException.class
    ),
    NOT_SERIALIZABLE_TRANSPORT_EXCEPTION(org.elasticsearch.transport.NotSerializableTransportException.class
    ),
    RESPONSE_HANDLER_FAILURE_TRANSPORT_EXCEPTION(org.elasticsearch.transport.ResponseHandlerFailureTransportException.class
    ),
    INDEX_CREATION_EXCEPTION(org.elasticsearch.indices.IndexCreationException.class
    ),
    INDEX_NOT_FOUND_EXCEPTION(IndexMissingException.class),
    ILLEGAL_SHARD_ROUTING_STATE_EXCEPTION(org.elasticsearch.cluster.routing.IllegalShardRoutingStateException.class
    ),
    BROADCAST_SHARD_OPERATION_FAILED_EXCEPTION(org.elasticsearch.action.support.broadcast.BroadcastShardOperationFailedException.class
    ),
    RESOURCE_NOT_FOUND_EXCEPTION(org.elasticsearch.ResourceNotFoundException.class
    ),
    ACTION_TRANSPORT_EXCEPTION(org.elasticsearch.transport.ActionTransportException.class
    ),
    ELASTICSEARCH_GENERATION_EXCEPTION(org.elasticsearch.ElasticsearchGenerationException.class
    ),
    //      22 was CreateFailedEngineException
    INDEX_SHARD_STARTED_EXCEPTION(org.elasticsearch.index.shard.IndexShardStartedException.class
    ),
    SEARCH_CONTEXT_MISSING_EXCEPTION(org.elasticsearch.search.SearchContextMissingException.class
    ),
    GENERAL_SCRIPT_EXCEPTION(org.elasticsearch.script.GeneralScriptException.class
    ),
    SNAPSHOT_CREATION_EXCEPTION(org.elasticsearch.snapshots.SnapshotCreationException.class
    ),
    DELETE_FAILED_ENGINE_EXCEPTION(org.elasticsearch.index.engine.DeleteFailedEngineException.class
    ),
    DOCUMENT_MISSING_EXCEPTION(org.elasticsearch.index.engine.DocumentMissingException.class
    ),
    SNAPSHOT_EXCEPTION(org.elasticsearch.snapshots.SnapshotException.class
    ),
    INVALID_ALIAS_NAME_EXCEPTION(org.elasticsearch.indices.InvalidAliasNameException.class
    ),
    INVALID_INDEX_NAME_EXCEPTION(org.elasticsearch.indices.InvalidIndexNameException.class
    ),
    INDEX_PRIMARY_SHARD_NOT_ALLOCATED_EXCEPTION(org.elasticsearch.indices.IndexPrimaryShardNotAllocatedException.class
    ),
    TRANSPORT_EXCEPTION(org.elasticsearch.transport.TransportException.class
    ),
    ELASTICSEARCH_PARSE_EXCEPTION(org.elasticsearch.ElasticsearchParseException.class
    ),
    SEARCH_EXCEPTION(org.elasticsearch.search.SearchException.class
    ),
    MAPPER_EXCEPTION(org.elasticsearch.index.mapper.MapperException.class),
    INVALID_TYPE_NAME_EXCEPTION(org.elasticsearch.indices.InvalidTypeNameException.class
    ),
    SNAPSHOT_RESTORE_EXCEPTION(org.elasticsearch.snapshots.SnapshotRestoreException.class
    ),
    PARSING_EXCEPTION(org.elasticsearch.common.ParsingException.class),
    INDEX_SHARD_CLOSED_EXCEPTION(org.elasticsearch.index.shard.IndexShardClosedException.class
    ),
    RECOVER_FILES_RECOVERY_EXCEPTION(org.elasticsearch.indices.recovery.RecoverFilesRecoveryException.class
    ),
    RECOVERY_FAILED_EXCEPTION(org.elasticsearch.indices.recovery.RecoveryFailedException.class
    ),
    INDEX_SHARD_RELOCATED_EXCEPTION(org.elasticsearch.index.shard.IndexShardRelocatedException.class
    ),
    NODE_SHOULD_NOT_CONNECT_EXCEPTION(org.elasticsearch.transport.NodeShouldNotConnectException.class
    ),
    INDEX_TEMPLATE_ALREADY_EXISTS_EXCEPTION(org.elasticsearch.indices.IndexTemplateAlreadyExistsException.class
    ),
    TRANSLOG_CORRUPTED_EXCEPTION(org.elasticsearch.index.translog.TranslogCorruptedException.class
    ),
    CLUSTER_BLOCK_EXCEPTION(org.elasticsearch.cluster.block.ClusterBlockException.class
    ),
    FETCH_PHASE_EXECUTION_EXCEPTION(org.elasticsearch.search.fetch.FetchPhaseExecutionException.class
    ),
    INDEX_SHARD_ALREADY_EXISTS_EXCEPTION(org.elasticsearch.index.IndexShardAlreadyExistsException.class
    ),
    VERSION_CONFLICT_ENGINE_EXCEPTION(org.elasticsearch.index.engine.VersionConflictEngineException.class
    ),
    ENGINE_EXCEPTION(org.elasticsearch.index.engine.EngineException.class),
    // 54 was DocumentAlreadyExistsException, which is superseded by VersionConflictEngineException
    NO_SUCH_NODE_EXCEPTION(org.elasticsearch.action.NoSuchNodeException.class),
    SETTINGS_EXCEPTION(org.elasticsearch.common.settings.SettingsException.class
    ),
    INDEX_TEMPLATE_MISSING_EXCEPTION(org.elasticsearch.indices.IndexTemplateMissingException.class
    ),
    SEND_REQUEST_TRANSPORT_EXCEPTION(org.elasticsearch.transport.SendRequestTransportException.class
    ),
    ES_REJECTED_EXECUTION_EXCEPTION(org.elasticsearch.common.util.concurrent.EsRejectedExecutionException.class
    ),
    EARLY_TERMINATION_EXCEPTION(org.elasticsearch.common.lucene.Lucene.EarlyTerminationException.class
    ),
    ALIAS_FILTER_PARSING_EXCEPTION(org.elasticsearch.indices.AliasFilterParsingException.class
    ),
    // 64 was DeleteByQueryFailedEngineException, which was removed in 3.0
    GATEWAY_EXCEPTION(org.elasticsearch.gateway.GatewayException.class),
    INDEX_SHARD_NOT_RECOVERING_EXCEPTION(org.elasticsearch.index.shard.IndexShardNotRecoveringException.class
    ),
    HTTP_EXCEPTION(org.elasticsearch.http.HttpException.class),
    ELASTICSEARCH_EXCEPTION(org.elasticsearch.ElasticsearchException.class
    ),
    SNAPSHOT_MISSING_EXCEPTION(org.elasticsearch.snapshots.SnapshotMissingException.class
    ),
    PRIMARY_MISSING_ACTION_EXCEPTION(org.elasticsearch.action.PrimaryMissingActionException.class
    ),
    FAILED_NODE_EXCEPTION(org.elasticsearch.action.FailedNodeException.class),
    SEARCH_PARSE_EXCEPTION(org.elasticsearch.search.SearchParseException.class),
    CONCURRENT_SNAPSHOT_EXECUTION_EXCEPTION(org.elasticsearch.snapshots.ConcurrentSnapshotExecutionException.class
    ),
    BLOB_STORE_EXCEPTION(org.elasticsearch.common.blobstore.BlobStoreException.class
    ),
    RECOVERY_ENGINE_EXCEPTION(org.elasticsearch.index.engine.RecoveryEngineException.class
    ),
    UNCATEGORIZED_EXECUTION_EXCEPTION(org.elasticsearch.common.util.concurrent.UncategorizedExecutionException.class
    ),
    TIMESTAMP_PARSING_EXCEPTION(org.elasticsearch.action.TimestampParsingException.class
    ),
    ROUTING_MISSING_EXCEPTION(org.elasticsearch.action.RoutingMissingException.class
    ),
    INDEX_FAILED_ENGINE_EXCEPTION(org.elasticsearch.index.engine.IndexFailedEngineException.class
    ),
    INDEX_SHARD_RESTORE_FAILED_EXCEPTION(org.elasticsearch.index.snapshots.IndexShardRestoreFailedException.class
    ),
    REPOSITORY_EXCEPTION(org.elasticsearch.repositories.RepositoryException.class
    ),
    RECEIVE_TIMEOUT_TRANSPORT_EXCEPTION(org.elasticsearch.transport.ReceiveTimeoutTransportException.class
    ),
    NODE_DISCONNECTED_EXCEPTION(org.elasticsearch.transport.NodeDisconnectedException.class
    ),
    ALREADY_EXPIRED_EXCEPTION(org.elasticsearch.index.AlreadyExpiredException.class
    ),
    AGGREGATION_EXECUTION_EXCEPTION(org.elasticsearch.search.aggregations.AggregationExecutionException.class
    ),
    // 87 used to be for MergeMappingException
    INVALID_INDEX_TEMPLATE_EXCEPTION(org.elasticsearch.indices.InvalidIndexTemplateException.class
    ),
    REFRESH_FAILED_ENGINE_EXCEPTION(org.elasticsearch.index.engine.RefreshFailedEngineException.class
    ),
    AGGREGATION_INITIALIZATION_EXCEPTION(org.elasticsearch.search.aggregations.AggregationInitializationException.class
    ),
    DELAY_RECOVERY_EXCEPTION(org.elasticsearch.indices.recovery.DelayRecoveryException.class
    ),
    // 93 used to be for IndexWarmerMissingException
    NO_NODE_AVAILABLE_EXCEPTION(org.elasticsearch.client.transport.NoNodeAvailableException.class
    ),
    INVALID_SNAPSHOT_NAME_EXCEPTION(org.elasticsearch.snapshots.InvalidSnapshotNameException.class
    ),
    ILLEGAL_INDEX_SHARD_STATE_EXCEPTION(org.elasticsearch.index.shard.IllegalIndexShardStateException.class
    ),
    INDEX_SHARD_SNAPSHOT_EXCEPTION(org.elasticsearch.index.snapshots.IndexShardSnapshotException.class
    ),
    INDEX_SHARD_NOT_STARTED_EXCEPTION(org.elasticsearch.index.shard.IndexShardNotStartedException.class
    ),
    SEARCH_PHASE_EXECUTION_EXCEPTION(org.elasticsearch.action.search.SearchPhaseExecutionException.class
    ),
    ACTION_NOT_FOUND_TRANSPORT_EXCEPTION(org.elasticsearch.transport.ActionNotFoundTransportException.class
    ),
    TRANSPORT_SERIALIZATION_EXCEPTION(org.elasticsearch.transport.TransportSerializationException.class
    ),
    REMOTE_TRANSPORT_EXCEPTION(org.elasticsearch.transport.RemoteTransportException.class
    ),
    ENGINE_CREATION_FAILURE_EXCEPTION(org.elasticsearch.index.engine.EngineCreationFailureException.class
    ),
    ROUTING_EXCEPTION(org.elasticsearch.cluster.routing.RoutingException.class
    ),
    REPOSITORY_MISSING_EXCEPTION(org.elasticsearch.repositories.RepositoryMissingException.class
    ),
    DOCUMENT_SOURCE_MISSING_EXCEPTION(org.elasticsearch.index.engine.DocumentSourceMissingException.class
    ),
    // 110 used to be FlushNotAllowedEngineException
    NO_CLASS_SETTINGS_EXCEPTION(org.elasticsearch.common.settings.NoClassSettingsException.class
    ),
    BIND_TRANSPORT_EXCEPTION(org.elasticsearch.transport.BindTransportException.class
    ),
    ALIASES_NOT_FOUND_EXCEPTION(org.elasticsearch.rest.action.admin.indices.AliasesNotFoundException.class
    ),
    INDEX_SHARD_RECOVERING_EXCEPTION(org.elasticsearch.index.shard.IndexShardRecoveringException.class
    ),
    TRANSLOG_EXCEPTION(org.elasticsearch.index.translog.TranslogException.class
    ),
    PROCESS_CLUSTER_EVENT_TIMEOUT_EXCEPTION(org.elasticsearch.cluster.metadata.ProcessClusterEventTimeoutException.class
    ),
    ELASTICSEARCH_TIMEOUT_EXCEPTION(org.elasticsearch.ElasticsearchTimeoutException.class
    ),
    QUERY_PHASE_EXECUTION_EXCEPTION(org.elasticsearch.search.query.QueryPhaseExecutionException.class
    ),
    REPOSITORY_VERIFICATION_EXCEPTION(org.elasticsearch.repositories.RepositoryVerificationException.class
    ),
    INDEX_ALREADY_EXISTS_EXCEPTION(org.elasticsearch.indices.IndexAlreadyExistsException.class
    ),
    // 124 used to be Script.ScriptParseException
    MAPPER_PARSING_EXCEPTION(org.elasticsearch.index.mapper.MapperParsingException.class
    ),
    SEARCH_CONTEXT_EXCEPTION(org.elasticsearch.search.SearchContextException.class
    ),
    SEARCH_SOURCE_BUILDER_EXCEPTION(org.elasticsearch.search.builder.SearchSourceBuilderException.class
    ),
    ENGINE_CLOSED_EXCEPTION(org.elasticsearch.index.engine.EngineClosedException.class
    ),
    NO_SHARD_AVAILABLE_ACTION_EXCEPTION(org.elasticsearch.action.NoShardAvailableActionException.class
    ),
    UNAVAILABLE_SHARDS_EXCEPTION(org.elasticsearch.action.UnavailableShardsException.class
    ),
    FLUSH_FAILED_ENGINE_EXCEPTION(org.elasticsearch.index.engine.FlushFailedEngineException.class
    ),
    CIRCUIT_BREAKING_EXCEPTION(org.elasticsearch.common.breaker.CircuitBreakingException.class
    ),
    NODE_NOT_CONNECTED_EXCEPTION(org.elasticsearch.transport.NodeNotConnectedException.class
    ),
    STRICT_DYNAMIC_MAPPING_EXCEPTION(org.elasticsearch.index.mapper.StrictDynamicMappingException.class
    ),
    TYPE_MISSING_EXCEPTION(org.elasticsearch.indices.TypeMissingException.class
    ),
    SCRIPT_EXCEPTION(org.elasticsearch.script.ScriptException.class),
    ACTION_REQUEST_VALIDATION_EXCEPTION(ActionRequestValidationException.class);



    static Map<String, ElasticsearchExceptionHandler> simpleNameMapping = Maps.newHashMap();
    static {
        for (ElasticsearchExceptionHandler handler : values()) {
            simpleNameMapping.put(handler.exceptionClass.getSimpleName(), handler);
        }
    }

    final Class<? extends ElasticsearchException> exceptionClass;

    <E extends ElasticsearchException> ElasticsearchExceptionHandler(Class<E> exceptionClass) {
        // We need the exceptionClass because you can't dig it out of the constructor reliably.
        this.exceptionClass = exceptionClass;
    }

    public static ElasticsearchExceptionHandler safeValueOf(String name) {
        if (name == null) {
            return UNCATEGORIZED_EXECUTION_EXCEPTION;
        }
        try {
            return valueOf(name.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            if (simpleNameMapping.containsKey(name)) {
                return simpleNameMapping.get(name);
            }
            return UNCATEGORIZED_EXECUTION_EXCEPTION;
        }
    }

    public ElasticsearchException newException(XContentObject error) throws ElasticsearchException {
        try {
            return this.exceptionClass.getDeclaredConstructor(XContentObject.class).newInstance(error);
        } catch (Exception e) {
            return new UncategorizedExecutionException(error);
        }

    }
}
