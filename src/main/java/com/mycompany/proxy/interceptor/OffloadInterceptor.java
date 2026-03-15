package com.mycompany.proxy.interceptor;

import com.mycompany.proxy.protocol.KafkaMessage;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.apache.kafka.common.protocol.ApiKeys;

import java.io.FileOutputStream;
import java.util.UUID;

public class OffloadInterceptor implements KafkaInterceptor {

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
        try (FileOutputStream out = new FileOutputStream(path)) {
            byte[] bytes = new byte[fullPayload.readableBytes()];
            fullPayload.getBytes(0, bytes);
            out.write(bytes);
            System.out.println("[OFFLOAD] Large payload (" + bytes.length + " bytes) offloaded to: " + path);
        } catch (Exception e) {
            System.err.println("[OFFLOAD] Failed to offload: " + e.getMessage());
        }
    }
}
