package com.mycompany.proxy.interceptor;

import com.mycompany.proxy.protocol.KafkaMessage;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.apache.kafka.common.protocol.ApiKeys;

import java.nio.charset.StandardCharsets;

public class JsonValidationInterceptor implements KafkaInterceptor {

    @Override
    public void onRequest(ChannelHandlerContext ctx, KafkaMessage message, KafkaInterceptorChain.Callback callback) {
        if (message.apiKey() == ApiKeys.PRODUCE.id) {
            String payload = extractValue(message);
            if (payload != null && !isValidJson(payload)) {
                System.out.println("[DATAQUALITY] Invalid JSON blocked: " + payload);
                callback.block();
                return;
            }
        }
        callback.proceed();
    }

    private String extractValue(KafkaMessage message) {
        ByteBuf buf = message.payload();
        int readerIndex = buf.readerIndex();
        try {
            buf.readShort(); buf.readShort(); buf.readInt();
            short clIdLen = buf.readShort();
            if (clIdLen > 0) buf.skipBytes(clIdLen);

            if (message.apiKey() == ApiKeys.PRODUCE.id) {
                buf.readShort(); buf.readInt(); buf.readInt();
                short nameLen = buf.readShort();
                buf.skipBytes(nameLen);
                buf.readInt();
                buf.readInt();
                int msgSetLen = buf.readInt();
                if (msgSetLen > 0) {
                    byte[] payload = new byte[buf.readableBytes()];
                    buf.readBytes(payload);
                    return new String(payload, StandardCharsets.UTF_8);
                }
            }
        } catch (Exception e) {} finally {
            buf.readerIndex(readerIndex);
        }
        return null;
    }

    private boolean isValidJson(String payload) {
        String trimmed = payload.trim();
        return (trimmed.startsWith("{") && trimmed.endsWith("}")) ||
               (trimmed.startsWith("[") && trimmed.endsWith("]"));
    }
}
