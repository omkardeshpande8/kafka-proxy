package com.example.proxy.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.RequestHeader;

import java.nio.ByteBuffer;
import java.util.List;

public class KafkaProtocolHandler extends MessageToMessageDecoder<ByteBuf> {

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) throws Exception {
        if (msg.readableBytes() < 2) {
            out.add(msg.retain());
            return;
        }

        boolean isFrontend = ctx.pipeline().get("frontendHandler") != null;

        if (isFrontend) {
            int readerIndex = msg.readerIndex();
            try {
                // Use Kafka's RequestHeader for robust decoding
                ByteBuffer nioBuffer = msg.nioBuffer();
                RequestHeader header = RequestHeader.parse(nioBuffer);

                int headerSize = nioBuffer.position();
                // We must NOT slice and retain separately if we want to modify the original payload in-place
                // Instead, let's keep the full message in KafkaMessage

                msg.readerIndex(readerIndex + headerSize);

                out.add(new KafkaMessage(msg.retain(),
                        header.apiKey().id,
                        header.apiVersion(),
                        header.correlationId(),
                        header.clientId(),
                        headerSize));
            } catch (Exception e) {
                msg.readerIndex(readerIndex);
                out.add(msg.retain());
            }
        } else {
            out.add(msg.retain());
        }
    }
}
