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
