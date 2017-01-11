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
package org.elasticsearch.common.util;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.elasticsearch.common.Strings;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.List;

/**
 */
public class UriBuilder {


    private final StringBuilder url;
    public static UriBuilder newBuilder() {
        return new UriBuilder();
    }

    private UriBuilder() {
        url = new StringBuilder();
    }

    public UriBuilder csv(String... pathParams) {
        if (pathParams == null || pathParams.length == 0) {
            return this;
        }
        url.append('/').append(Joiner.on(',').skipNulls().join(encodeParams(pathParams)));
        return this;
    }

    public UriBuilder csvOrDefault(String defaultValue, String... pathParams) {
        if (pathParams == null || pathParams.length == 0) {
            url.append('/').append(urlEncode(defaultValue));
            return this;
        }
        return csv(pathParams);
    }

    public String build() {
        return url.toString();
    }

    public UriBuilder slash(String... pathParams) {
        url.append('/').append(Joiner.on('/').skipNulls().join(encodeParams(pathParams)));
        return this;
    }

    private List<String> encodeParams(String[] pathParams) {
        List<String> encodedParams = Lists.newArrayListWithCapacity(pathParams.length);
        for (String pathParam : pathParams) {
            encodedParams.add(urlEncode(pathParam));
        }
        return encodedParams;
    }
    private String urlEncode(String value)  {
        try {
            return URLEncoder.encode(value, Strings.UTF8);
        } catch (UnsupportedEncodingException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }

    @Override
    public String toString() {
        return build();
    }
}
