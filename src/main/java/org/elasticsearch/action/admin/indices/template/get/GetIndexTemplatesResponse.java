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
package org.elasticsearch.action.admin.indices.template.get;

import com.google.common.collect.Lists;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentObject;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 *
 */
public class GetIndexTemplatesResponse extends ActionResponse {

    private List<IndexTemplateMetaData> indexTemplates;

    GetIndexTemplatesResponse() {
    }

    GetIndexTemplatesResponse(List<IndexTemplateMetaData> indexTemplates) {
        this.indexTemplates = indexTemplates;
    }

    public List<IndexTemplateMetaData> getIndexTemplates() {
        return indexTemplates;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        int size = in.readVInt();
        indexTemplates = Lists.newArrayListWithExpectedSize(size);
        for (int i = 0 ; i < size ; i++) {
            indexTemplates.add(0, IndexTemplateMetaData.Builder.readFrom(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(indexTemplates.size());
        for (IndexTemplateMetaData indexTemplate : indexTemplates) {
            IndexTemplateMetaData.Builder.writeTo(indexTemplate, out);
        }
    }

    @Override
    public void readFrom(XContentObject source) throws IOException {

        Set<String> templateNames = source.keySet();
        indexTemplates = new ArrayList<>(templateNames.size());
        for (String templateName : templateNames) {
            XContentParser parser = source.getAsXContentParserEntry(templateName);
            parser.nextToken(); // skip init
            parser.nextToken(); // skip start_object
            IndexTemplateMetaData e = IndexTemplateMetaData.Builder.fromXContent(parser);
            indexTemplates.add(e);
        }
    }
}
