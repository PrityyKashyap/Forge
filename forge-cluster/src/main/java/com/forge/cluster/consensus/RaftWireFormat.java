package com.forge.cluster.consensus;

import com.forge.cluster.NodeId;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * {@link RaftRpcServer}/{@link RaftCluster}'s wire format — deliberately
 * separate from both {@code forge-common}'s client protocol and
 * {@code ReplicationWireFormat}, for the same reason those two are
 * separate from each other: a different message shape belonging to a
 * different logical protocol. One connection carries exactly one
 * request/response pair: the client writes a one-byte message-type
 * discriminator followed by the request body, the server writes back only
 * the matching response body (no discriminator needed on the way back —
 * the connection's own context already establishes which kind of response
 * it is), then both sides close.
 */
final class RaftWireFormat {

    static final byte MSG_REQUEST_VOTE = 1;
    static final byte MSG_APPEND_ENTRIES = 2;

    private static final int MAX_COMMAND_LENGTH = 1024;
    private static final int MAX_ENTRY_COUNT = 1_000_000;

    private RaftWireFormat() {
    }

    static void writeRequestVoteRequest(DataOutputStream out, RequestVoteRequest request) throws IOException {
        out.writeLong(request.term());
        out.writeUTF(request.candidateId().value());
        out.writeLong(request.lastLogIndex());
        out.writeLong(request.lastLogTerm());
    }

    static RequestVoteRequest readRequestVoteRequest(DataInputStream in) throws IOException {
        long term = in.readLong();
        NodeId candidateId = new NodeId(in.readUTF());
        long lastLogIndex = in.readLong();
        long lastLogTerm = in.readLong();
        return new RequestVoteRequest(term, candidateId, lastLogIndex, lastLogTerm);
    }

    static void writeRequestVoteResponse(DataOutputStream out, RequestVoteResponse response) throws IOException {
        out.writeLong(response.term());
        out.writeBoolean(response.voteGranted());
    }

    static RequestVoteResponse readRequestVoteResponse(DataInputStream in) throws IOException {
        long term = in.readLong();
        boolean voteGranted = in.readBoolean();
        return new RequestVoteResponse(term, voteGranted);
    }

    static void writeAppendEntriesRequest(DataOutputStream out, AppendEntriesRequest request) throws IOException {
        out.writeLong(request.term());
        out.writeUTF(request.leaderId().value());
        out.writeLong(request.prevLogIndex());
        out.writeLong(request.prevLogTerm());
        out.writeInt(request.entries().size());
        for (LogEntry entry : request.entries()) {
            out.writeLong(entry.term());
            out.writeUTF(entry.command());
        }
        out.writeLong(request.leaderCommit());
    }

    static AppendEntriesRequest readAppendEntriesRequest(DataInputStream in) throws IOException {
        long term = in.readLong();
        NodeId leaderId = new NodeId(in.readUTF());
        long prevLogIndex = in.readLong();
        long prevLogTerm = in.readLong();
        int entryCount = in.readInt();
        if (entryCount < 0 || entryCount > MAX_ENTRY_COUNT) {
            throw new IOException("corrupt Raft AppendEntries request: invalid entry count " + entryCount);
        }
        List<LogEntry> entries = new ArrayList<>(entryCount);
        for (int i = 0; i < entryCount; i++) {
            long entryTerm = in.readLong();
            String command = in.readUTF();
            if (command.length() > MAX_COMMAND_LENGTH) {
                throw new IOException("corrupt Raft AppendEntries request: command too long");
            }
            entries.add(new LogEntry(entryTerm, command));
        }
        long leaderCommit = in.readLong();
        return new AppendEntriesRequest(term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit);
    }

    static void writeAppendEntriesResponse(DataOutputStream out, AppendEntriesResponse response) throws IOException {
        out.writeLong(response.term());
        out.writeBoolean(response.success());
        out.writeLong(response.matchedIndex());
    }

    static AppendEntriesResponse readAppendEntriesResponse(DataInputStream in) throws IOException {
        long term = in.readLong();
        boolean success = in.readBoolean();
        long matchedIndex = in.readLong();
        return new AppendEntriesResponse(term, success, matchedIndex);
    }
}
