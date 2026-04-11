package com.mycompany.proxy.interceptor;

import com.mycompany.proxy.protocol.KafkaMessage;
import io.netty.channel.ChannelHandlerContext;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

public class VirtualSqlInterceptor implements KafkaInterceptor {
    private static final Logger logger = LoggerFactory.getLogger(VirtualSqlInterceptor.class);

    private final String filterValue;

    public VirtualSqlInterceptor(String filterValue) {
        this.filterValue = filterValue;
    }

    @Override
    public void onRequest(ChannelHandlerContext ctx, KafkaMessage message, KafkaInterceptorChain.Callback callback) {
        if (message.apiKey() == ApiKeys.PRODUCE.id) {
            String payload = extractValue(message);
            if (payload != null && !payload.contains(filterValue)) {
                logger.info("[SQLFILTER] Dropping record as it doesn't match criteria: {}", filterValue);
                callback.block();
                return;
            }
        }
        callback.proceed();
    }

    private String extractValue(KafkaMessage message) {
        try {
            ProduceRequest produceRequest = (ProduceRequest) AbstractRequest.parseRequest(
                    ApiKeys.PRODUCE, message.apiVersion(), message.body().nioBuffer()).request;
            for (org.apache.kafka.common.message.ProduceRequestData.TopicProduceData tpd : produceRequest.data().topicData()) {
                for (org.apache.kafka.common.message.ProduceRequestData.PartitionProduceData ppd : tpd.partitionData()) {
                    if (ppd.records() != null && ppd.records() instanceof org.apache.kafka.common.record.Records) {
                        for (Record record : ((org.apache.kafka.common.record.Records) ppd.records()).records()) {
                            if (record.hasValue()) {
                                return StandardCharsets.UTF_8.decode(record.value()).toString();
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.error("[SQLFILTER] Failed to parse ProduceRequest: {}", e.getMessage());
        }
        return null;
    }
}
