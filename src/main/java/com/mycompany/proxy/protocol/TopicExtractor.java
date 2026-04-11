package com.mycompany.proxy.protocol;

import io.netty.buffer.ByteBuf;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.FetchRequest;

import java.nio.charset.StandardCharsets;

public final class TopicExtractor {

    private TopicExtractor() {
    }

    public static String extractTopic(KafkaMessage message) {
        try {
            if (message.apiKey() == ApiKeys.PRODUCE.id) {
                ProduceRequest req = (ProduceRequest) AbstractRequest.parseRequest(
                        ApiKeys.PRODUCE, message.apiVersion(), message.body().nioBuffer()).request;
                if (!req.data().topicData().isEmpty()) {
                    return req.data().topicData().iterator().next().name();
                }
            } else if (message.apiKey() == ApiKeys.FETCH.id) {
                FetchRequest req = (FetchRequest) AbstractRequest.parseRequest(
                        ApiKeys.FETCH, message.apiVersion(), message.body().nioBuffer()).request;
                if (!req.data().topics().isEmpty()) {
                    return req.data().topics().iterator().next().topic();
                }
            }
        } catch (Exception e) {
            // Log and fallback? For extraction we return null if failed
        }

        return null;
    }
}
