package com.example.proxy.interceptor;

import com.example.proxy.protocol.KafkaMessage;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.apache.kafka.common.protocol.ApiKeys;

import java.util.HashMap;
import java.util.Map;

public class TopicAliasInterceptor implements KafkaInterceptor {

    private final Map<String, String> virtualToPhysical = new HashMap<>();

    public void addAlias(String virtual, String physical) {
        virtualToPhysical.put(virtual, physical);
    }

    @Override
    public void onRequest(ChannelHandlerContext ctx, KafkaMessage message, KafkaInterceptorChain.Callback callback) {
        if (message.apiKey() == ApiKeys.PRODUCE.id || message.apiKey() == ApiKeys.FETCH.id) {
            modifyTopic(message);
        }
        callback.proceed();
    }

    private void modifyTopic(KafkaMessage message) {
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
                    int topicNameIndex = payload.readerIndex();
                    short topicNameLen = payload.readShort();
                    byte[] topicBytes = new byte[topicNameLen];
                    payload.readBytes(topicBytes);
                    String virtualTopic = new String(topicBytes);

                    if (virtualToPhysical.containsKey(virtualTopic)) {
                        String physicalTopic = virtualToPhysical.get(virtualTopic);
                        // NOTE: In-place replacement only works if physical and virtual names have the same length.
                        // Full re-serialization would be required for different lengths.
                        if (physicalTopic.length() == virtualTopic.length()) {
                            payload.setBytes(topicNameIndex + 2, physicalTopic.getBytes());
                            System.out.println(String.format("[ALIAS] Remapped %s -> %s", virtualTopic, physicalTopic));
                        } else {
                            System.err.println(String.format("[ALIAS] Skipping alias %s -> %s due to length mismatch (In-place replacement requirement)", virtualTopic, physicalTopic));
                        }
                    }
                }
            }
        } catch (Exception e) {} finally {
            payload.readerIndex(readerIndex);
        }
    }
}
