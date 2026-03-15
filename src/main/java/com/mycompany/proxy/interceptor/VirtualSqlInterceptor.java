package com.mycompany.proxy.interceptor;

import com.mycompany.proxy.protocol.KafkaMessage;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.apache.kafka.common.protocol.ApiKeys;

import java.nio.charset.StandardCharsets;

public class VirtualSqlInterceptor implements KafkaInterceptor {

    private final String filterValue;

    public VirtualSqlInterceptor(String filterValue) {
        this.filterValue = filterValue;
    }

    @Override
    public void onRequest(ChannelHandlerContext ctx, KafkaMessage message, KafkaInterceptorChain.Callback callback) {
        if (message.apiKey() == ApiKeys.PRODUCE.id) {
            String payload = extractValue(message);
            if (payload != null && !payload.contains(filterValue)) {
                System.out.println("[SQLFILTER] Dropping record as it doesn't match criteria: " + filterValue);
                // In a real Virtual SQL implementation, we would filter specific records
                // from the RecordBatch. For this POC, we drop the whole request if no match.
                callback.block();
                return;
            }
        }
        callback.proceed();
    }

    private String extractValue(KafkaMessage message) {
        ByteBuf buf = message.body();
        int readerIndex = buf.readerIndex();
        try {
            if (message.apiKey() == ApiKeys.PRODUCE.id) {
                // Simplified extraction for POC
                buf.readShort(); buf.readInt(); buf.readInt();
                short nameLen = buf.readShort(); buf.skipBytes(nameLen);
                buf.readInt(); buf.readInt();
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
}
