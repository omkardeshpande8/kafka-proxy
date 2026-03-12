package com.example.proxy.interceptor;

import com.example.proxy.protocol.KafkaMessage;
import com.example.proxy.protocol.TopicExtractor;
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
            String topic = TopicExtractor.extractTopic(message);
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
}
