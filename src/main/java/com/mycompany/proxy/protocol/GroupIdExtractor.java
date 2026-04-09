package com.mycompany.proxy.protocol;

import io.netty.buffer.ByteBuf;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.*;

import java.nio.ByteBuffer;

public final class GroupIdExtractor {

    private GroupIdExtractor() {}

    public static String extractGroupId(KafkaMessage message) {
        ApiKeys apiKey = ApiKeys.forId(message.apiKey());
        ByteBuf payload = message.payload();
        ByteBuffer nioBuffer = payload.nioBuffer();

        try {
            RequestHeader header = RequestHeader.parse(nioBuffer);
            AbstractRequest request = AbstractRequest.parseRequest(apiKey, message.apiVersion(), nioBuffer).request;

            if (request instanceof JoinGroupRequest) {
                return ((JoinGroupRequest) request).data().groupId();
            } else if (request instanceof SyncGroupRequest) {
                return ((SyncGroupRequest) request).data().groupId();
            } else if (request instanceof HeartbeatRequest) {
                return ((HeartbeatRequest) request).data().groupId();
            } else if (request instanceof LeaveGroupRequest) {
                return ((LeaveGroupRequest) request).data().groupId();
            } else if (request instanceof OffsetCommitRequest) {
                return ((OffsetCommitRequest) request).data().groupId();
            } else if (request instanceof OffsetFetchRequest) {
                if (((OffsetFetchRequest) request).data().groups() != null && !((OffsetFetchRequest) request).data().groups().isEmpty()) {
                    return ((OffsetFetchRequest) request).data().groups().iterator().next().groupId();
                }
                return ((OffsetFetchRequest) request).groupId();
            }
        } catch (Exception e) {
            // Ignore and return null
        }
        return null;
    }
}
