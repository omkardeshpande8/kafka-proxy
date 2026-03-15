package com.mycompany.proxy.interceptor;

import com.mycompany.proxy.protocol.KafkaMessage;
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
    public void onRequest(ChannelHandlerContext ctx, KafkaMessage message, KafkaInterceptorChain.Callback callback) {
        if (message.apiKey() == ApiKeys.PRODUCE.id || message.apiKey() == ApiKeys.FETCH.id) {
            String topic = extractTopic(message);
            if (topic != null) {
                for (Pattern pattern : blockedTopicPatterns) {
                    if (pattern.matcher(topic).matches()) {
                        System.out.println("[GUARDRAIL] Blocked request for topic: " + topic);
                        callback.block();
                        return;
                    }
                }
            }
        }
        callback.proceed();
    }

    private String extractTopic(KafkaMessage message) {
        ByteBuf payload = message.body();
        int readerIndex = payload.readerIndex();
        try {
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
        } catch (Exception e) {} finally {
            payload.readerIndex(readerIndex);
        }
        return null;
    }
}
