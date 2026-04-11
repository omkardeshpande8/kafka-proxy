package com.mycompany.proxy.interceptor;

import com.mycompany.proxy.protocol.KafkaMessage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.RequestHeader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class TopicAliasInterceptor implements KafkaInterceptor {
    private static final Logger logger = LoggerFactory.getLogger(TopicAliasInterceptor.class);

    private final Map<String, String> virtualToPhysical = new HashMap<>();

    public void addAlias(String virtual, String physical) {
        virtualToPhysical.put(virtual, physical);
    }

    @Override
    public void onRequest(ChannelHandlerContext ctx, KafkaMessage message, KafkaInterceptorChain.Callback callback) {
        if (message.apiKey() == ApiKeys.PRODUCE.id || message.apiKey() == ApiKeys.FETCH.id) {
            modifyTopic(ctx, message);
        }
        callback.proceed();
    }

    private void modifyTopic(ChannelHandlerContext ctx, KafkaMessage message) {
        try {
            ApiKeys apiKey = ApiKeys.forId(message.apiKey());
            ByteBuffer bodyBuffer = message.body().nioBuffer();
            // Duplicate to avoid position side-effects if needed, though parseRequest should be fine
            AbstractRequest request = AbstractRequest.parseRequest(apiKey, message.apiVersion(), bodyBuffer.duplicate()).request;

            boolean modified = false;
            if (request instanceof ProduceRequest) {
                ProduceRequest produceRequest = (ProduceRequest) request;
                for (ProduceRequestData.TopicProduceData topicData : produceRequest.data().topicData()) {
                    if (virtualToPhysical.containsKey(topicData.name())) {
                        String physical = virtualToPhysical.get(topicData.name());
                        topicData.setName(physical);
                        modified = true;
                    }
                }
            } else if (request instanceof FetchRequest) {
                FetchRequest fetchRequest = (FetchRequest) request;
                for (FetchRequestData.FetchTopic topicData : fetchRequest.data().topics()) {
                    if (virtualToPhysical.containsKey(topicData.topic())) {
                        String physical = virtualToPhysical.get(topicData.topic());
                        topicData.setTopic(physical);
                        modified = true;
                    }
                }
            }

            if (modified) {
                // Re-serialize with the same header
                ByteBuffer payloadBuffer = message.payload().nioBuffer();
                RequestHeader header = RequestHeader.parse(payloadBuffer);

                ByteBuffer newFullBuffer = request.serializeWithHeader(header);
                if (newFullBuffer.remaining() > Integer.MAX_VALUE - 4) {
                    throw new IllegalStateException("Serialized request too large: " + newFullBuffer.remaining());
                }

                ByteBuf newPayload = Unpooled.wrappedBuffer(newFullBuffer);

                message.replacePayload(newPayload, header.size());
                logger.info("[ALIAS] Remapped topics in {} ({})", apiKey, message.clientId());
            }
        } catch (Exception e) {
            logger.warn("[ALIAS] Failed to remap topic: {}", e.getMessage(), e);
        }
    }
}
