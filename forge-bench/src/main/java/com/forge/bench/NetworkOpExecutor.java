package com.forge.bench;

import com.forge.client.ForgeClient;

import java.io.IOException;

/** Executes operations over a real {@link ForgeClient} connection (loopback TCP). */
final class NetworkOpExecutor implements OpExecutor {

    private final ForgeClient client;

    NetworkOpExecutor(String host, int port) throws IOException {
        this.client = ForgeClient.connect(host, port);
    }

    @Override
    public void execute(BenchOperation op) throws IOException {
        switch (op) {
            case BenchOperation.Get get -> client.get(get.key());
            case BenchOperation.Put put -> client.put(put.key(), put.value());
            case BenchOperation.Delete delete -> client.delete(delete.key());
        }
    }

    @Override
    public void close() throws IOException {
        client.close();
    }
}
