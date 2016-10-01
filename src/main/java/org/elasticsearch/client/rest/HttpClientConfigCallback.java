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
package org.elasticsearch.client.rest;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.nio.conn.SchemeIOSessionStrategy;
import org.elasticsearch.client.rest.support.InternalRestClient;
import org.elasticsearch.client.rest.support.InternalRestClientBuilder;

/**
 * Callback used to customize the {@link CloseableHttpClient} instance used by a {@link InternalRestClient} instance.
 * Allows to customize default {@link RequestConfig} being set to the client and any parameter that
 * can be set through {@link HttpClientBuilder}
 */
public interface HttpClientConfigCallback {
    /**
     * Allows to customize the {@link CloseableHttpAsyncClient} being created and used by the {@link InternalRestClient}.
     * Commonly used to customize the default {@link org.apache.http.client.CredentialsProvider} for authentication
     * or the {@link SchemeIOSessionStrategy} for communication through ssl without losing any other useful default
     * value that the {@link InternalRestClientBuilder} internally sets, like connection pooling.
     */
    HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder);
}
