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

package org.elasticsearch.common.xcontent;

import org.elasticsearch.Version;

import java.util.Collections;

/**
 * @author rahulanishetty
 * @since 05/01/17.
 */
public class ToXContentUtils {

    public static final String TARGET_CLUSTER_VERSION_KEY = "VERSION";

    public static ToXContent.Params createParamsWithTargetClusterVersion(Version version, ToXContent.Params params) {
        if (version == null) {
            return ToXContent.EMPTY_PARAMS;
        }
        return new ToXContent.DelegatingMapParams(Collections.singletonMap(TARGET_CLUSTER_VERSION_KEY, version.toString()), params);
    }

    public static ToXContent.Params createParamsWithTargetClusterVersion(Version version) {
        return createParamsWithTargetClusterVersion(version, ToXContent.EMPTY_PARAMS);
    }

    public static Version getVersionFromParams(ToXContent.Params params){
        return Version.fromString(params.param(TARGET_CLUSTER_VERSION_KEY, Version.V_1_4_1.toString()));
    }

}
