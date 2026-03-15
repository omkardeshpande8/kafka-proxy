package com.mycompany.proxy.interceptor;

import com.mycompany.proxy.protocol.KafkaMessage;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.apache.kafka.common.protocol.ApiKeys;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

public class FieldMaskingInterceptor implements KafkaInterceptor {

    private final Set<String> maskedFields = new HashSet<>();

    public void addMaskedField(String field) {
        maskedFields.add(field);
    }

    @Override
    public void onRequest(ChannelHandlerContext ctx, KafkaMessage message, KafkaInterceptorChain.Callback callback) {
        if (message.apiKey() == ApiKeys.PRODUCE.id) {
            maskFields(message);
        }
        callback.proceed();
    }

    private void maskFields(KafkaMessage message) {
        ByteBuf buf = message.body();
        int readerIndex = buf.readerIndex();
        try {
            // Body starts after header
            if (message.apiKey() == ApiKeys.PRODUCE.id) {
                buf.readShort(); buf.readInt(); buf.readInt();
                short nameLen = buf.readShort(); buf.skipBytes(nameLen);
                buf.readInt(); buf.readInt();
                int msgSetLen = buf.readInt();
                if (msgSetLen > 0) {
                    int payloadIndex = buf.readerIndex();
                    byte[] payload = new byte[buf.readableBytes()];
                    buf.readBytes(payload);
                    String json = new String(payload, StandardCharsets.UTF_8);

                    String originalJson = json;
                    for (String field : maskedFields) {
                        json = json.replaceAll("\"" + field + "\":\"[^\"]+\"", "\"" + field + "\":\"*****\"");
                    }

                    if (!json.equals(originalJson)) {
                        System.out.println("[MASKING] Field(s) masked in JSON payload");
                        // Use setBytes to avoid moving indices if length is same
                        buf.setBytes(payloadIndex, json.getBytes(StandardCharsets.UTF_8));
                    }
                }
            }
        } catch (Exception e) {} finally {
            buf.readerIndex(readerIndex);
        }
    }
}
