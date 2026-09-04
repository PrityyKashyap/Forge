package com.forge.cluster;

import java.util.Objects;

/** A stable, human-assigned identifier for one FORGE storage node (e.g. {@code "node-a"}). */
public record NodeId(String value) {

    public NodeId {
        Objects.requireNonNull(value, "value must not be null");
        if (value.isEmpty()) {
            throw new IllegalArgumentException("value must not be empty");
        }
    }

    @Override
    public String toString() {
        return value;
    }
}
