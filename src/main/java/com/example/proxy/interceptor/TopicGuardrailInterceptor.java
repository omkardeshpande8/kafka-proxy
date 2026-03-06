package com.example.proxy.interceptor;

import com.example.proxy.protocol.KafkaMessage;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.apache.kafka.common.protocol.ApiKeys;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

public class TopicGuardrailInterceptor implements KafkaInterceptor {

    private final Set<Pattern> blockedTopicPatterns = new HashSet<>();

    public void blockTopic(String pattern) {
        blockedTopicPatterns.add(Pattern.compile(pattern));
    }

    @Override
    public boolean onRequest(ChannelHandlerContext ctx, KafkaMessage message) {
        if (message.apiKey() == ApiKeys.PRODUCE.id || message.apiKey() == ApiKeys.FETCH.id) {
            String topic = extractTopic(message);
            if (topic != null) {
                for (Pattern pattern : blockedTopicPatterns) {
                    if (pattern.matcher(topic).matches()) {
                        System.out.println("[GUARDRAIL] Blocked request for topic: " + topic);
                        return false;
                    }
                }
            }
        }
        return true;
    }

    private String extractTopic(KafkaMessage message) {
        ByteBuf payload = message.payload();
        int readerIndex = payload.readerIndex();
        try {
            // Skip Header (already read by KafkaProtocolHandler, but payload still has it)
            // Header: apiKey(2), apiVersion(2), correlationId(4), clientIdLen(2), clientId(N)
            payload.readShort(); // apiKey
            payload.readShort(); // apiVersion
            payload.readInt();   // correlationId
            short clientIdLen = payload.readShort();
            if (clientIdLen > 0) payload.skipBytes(clientIdLen);

            // Now we are at the request body.
            // For ProduceRequest:
            // v0: transactional_id(null), acks(2), timeout(4), topics_array_len(4)
            if (message.apiKey() == ApiKeys.PRODUCE.id) {
                if (message.apiVersion() >= 3) {
                    short transIdLen = payload.readShort();
                    if (transIdLen > 0) payload.skipBytes(transIdLen);
                }
                payload.readShort(); // acks
                payload.readInt();   // timeout
                int topicsLen = payload.readInt();
                if (topicsLen > 0) {
                    short topicNameLen = payload.readShort();
                    byte[] topicBytes = new byte[topicNameLen];
                    payload.readBytes(topicBytes);
                    return new String(topicBytes);
                }
            }
            // For simplicity, we only implement basic extraction for Produce v0-v2
        } catch (Exception e) {
            // Extraction failed
        } finally {
            payload.readerIndex(readerIndex);
        }
        return null;
    }

    @Override
    public void onResponse(ChannelHandlerContext ctx, Object response) {
    }
}
