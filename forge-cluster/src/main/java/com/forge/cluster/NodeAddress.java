package com.forge.cluster;

import java.util.Objects;

/** Where a node's {@code ForgeServer} is actually listening. */
public record NodeAddress(String host, int port) {

    public NodeAddress {
        Objects.requireNonNull(host, "host must not be null");
        if (port <= 0 || port > 65535) {
            throw new IllegalArgumentException("port must be in (0, 65535]: " + port);
        }
    }

    @Override
    public String toString() {
        return host + ":" + port;
    }
}
