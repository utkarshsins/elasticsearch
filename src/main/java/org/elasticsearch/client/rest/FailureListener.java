package org.elasticsearch.client.rest;

import org.apache.http.HttpHost;

/**
 * Listener that allows to be notified whenever a failure happens. Useful when sniffing is enabled, so that we can sniff on failure.
 * The default implementation is a no-op.
 */
public class FailureListener {
    /**
     * Notifies that the host provided as argument has just failed
     */
    public void onFailure(HttpHost host) {

    }
}
