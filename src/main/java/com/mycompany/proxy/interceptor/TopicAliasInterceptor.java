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

import java.nio.ByteBuffer;
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
            modifyTopic(ctx, message);
        }
        callback.proceed();
    }

    private void modifyTopic(ChannelHandlerContext ctx, KafkaMessage message) {
        try {
            ApiKeys apiKey = ApiKeys.forId(message.apiKey());
            ByteBuffer bodyBuffer = message.body().nioBuffer();
            AbstractRequest request = AbstractRequest.parseRequest(apiKey, message.apiVersion(), bodyBuffer).request;

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
                ByteBuf newPayload = Unpooled.wrappedBuffer(newFullBuffer);

                message.replacePayload(newPayload, header.size());
                System.out.println("[ALIAS] Remapped topics in " + apiKey + " (" + message.clientId() + ")");
            }
        } catch (Exception e) {
            System.err.println("[ALIAS] Warning: Failed to remap topic: " + e.getMessage());
        }
    }
}
