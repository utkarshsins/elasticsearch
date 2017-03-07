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

package org.elasticsearch.action.admin.indices.stats;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.elasticsearch.cluster.routing.ImmutableShardRouting;
import org.elasticsearch.common.xcontent.XContentObject;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 */
public class IndexStats implements Iterable<IndexShardStats> {

    private final String index;

    private final ShardStats shards[];

    public IndexStats(String index, ShardStats[] shards) {
        this.index = index;
        this.shards = shards;
    }


    public IndexStats(String index, XContentObject in) throws IOException {
        this.index = index;
        XContentObject shards = in.getAsXContentObject("shards");
        if (shards != null) {
            this.shards = new ShardStats[shards.size()];
            for (String shardIdStr : shards.keySet()) {
                int shardId = Integer.parseInt(shardIdStr);
                List<XContentObject> shardStatsXContents = shards.getAsXContentObjectsOrEmpty(shardIdStr);
                if(!shardStatsXContents.isEmpty()) {
                    ShardStats shardStats = new ShardStats();
                    XContentObject shardStatsXContent = shardStatsXContents.get(0);
                    shardStats.shardRouting = new ImmutableShardRouting(index, shardId, shardStatsXContent.getAsXContentObject("routing"), 0);
                    shardStats.stats = new CommonStats();
                    shardStats.stats.readFrom(shardStatsXContent);
                    this.shards[shardId] = shardStats;
                }
            }
        } else {
            this.shards = new ShardStats[0];
        }
        this.primary = new CommonStats();
        this.primary.readFrom(in.getAsXContentObject("primaries"));

        this.total = new CommonStats();
        this.total.readFrom(in.getAsXContentObject("total"));
    }

    public String getIndex() {
        return this.index;
    }

    public ShardStats[] getShards() {
        return this.shards;
    }

    private Map<Integer, IndexShardStats> indexShards;

    public Map<Integer, IndexShardStats> getIndexShards() {
        if (indexShards != null) {
            return indexShards;
        }
        Map<Integer, List<ShardStats>> tmpIndexShards = Maps.newHashMap();
        for (ShardStats shard : shards) {
            List<ShardStats> lst = tmpIndexShards.get(shard.getShardRouting().id());
            if (lst == null) {
                lst = Lists.newArrayList();
                tmpIndexShards.put(shard.getShardRouting().id(), lst);
            }
            lst.add(shard);
        }
        indexShards = Maps.newHashMap();
        for (Map.Entry<Integer, List<ShardStats>> entry : tmpIndexShards.entrySet()) {
            indexShards.put(entry.getKey(), new IndexShardStats(entry.getValue().get(0).getShardRouting().shardId(), entry.getValue().toArray(new ShardStats[entry.getValue().size()])));
        }
        return indexShards;
    }

    @Override
    public Iterator<IndexShardStats> iterator() {
        return getIndexShards().values().iterator();
    }

    private CommonStats total = null;

    public CommonStats getTotal() {
        if (total != null) {
            return total;
        }
        CommonStats stats = new CommonStats();
        for (ShardStats shard : shards) {
            stats.add(shard.getStats());
        }
        total = stats;
        return stats;
    }

    private CommonStats primary = null;

    public CommonStats getPrimaries() {
        if (primary != null) {
            return primary;
        }
        CommonStats stats = new CommonStats();
        for (ShardStats shard : shards) {
            if (shard.getShardRouting().primary()) {
                stats.add(shard.getStats());
            }
        }
        primary = stats;
        return stats;
    }
}
