package com.mycompany.proxy.interceptor;

import com.mycompany.proxy.protocol.KafkaMessage;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.apache.kafka.common.protocol.ApiKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.util.UUID;

public class OffloadInterceptor implements KafkaInterceptor {
    private static final Logger logger = LoggerFactory.getLogger(OffloadInterceptor.class);

    private final int sizeThreshold;

    public OffloadInterceptor(int sizeThreshold) {
        this.sizeThreshold = sizeThreshold;
    }

    @Override
    public void onRequest(ChannelHandlerContext ctx, KafkaMessage message, KafkaInterceptorChain.Callback callback) {
        if (message.apiKey() == ApiKeys.PRODUCE.id) {
            int totalSize = message.payload().readableBytes();
            if (totalSize > sizeThreshold) {
                offloadPayload(message);
            }
        }
        callback.proceed();
    }

    private void offloadPayload(KafkaMessage message) {
        String offloadId = UUID.randomUUID().toString();
        String path = "/tmp/kafka-offload-" + offloadId;

        ByteBuf fullPayload = message.payload();
        int readerIndex = fullPayload.readerIndex();
        try (FileOutputStream out = new FileOutputStream(path)) {
            byte[] bytes = new byte[fullPayload.readableBytes()];
            fullPayload.getBytes(readerIndex, bytes);
            out.write(bytes);
            logger.info("[OFFLOAD] Large payload ({} bytes) offloaded to: {}", bytes.length, path);
            // NOTE: In a complete implementation, we would rewrite the ProduceRequest
            // to replace the actual records with an "offload reference" or similar.
        } catch (Exception e) {
            logger.error("[OFFLOAD] Failed to offload: {}", e.getMessage());
        }
    }
}
