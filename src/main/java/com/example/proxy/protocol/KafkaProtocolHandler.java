package com.example.proxy.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import java.util.List;

public class KafkaProtocolHandler extends MessageToMessageDecoder<ByteBuf> {

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) throws Exception {
        // Kafka message format: [4 bytes length] [payload]
        // Netty's LengthFieldBasedFrameDecoder already stripped the length field.

        if (msg.readableBytes() < 2) {
            out.add(msg.retain());
            return;
        }

        // Mark current position to reset if it's not a Request (e.g. Response from broker)
        // Actually, we only want to decode requests from client.
        // For responses, we might need a different handler or check if we are on the frontend.

        // Check if we are in the Frontend (Client to Proxy)
        boolean isFrontend = ctx.pipeline().get("frontendHandler") != null;

        if (isFrontend) {
            int readerIndex = msg.readerIndex();
            try {
                short apiKey = msg.readShort();
                short apiVersion = msg.readShort();
                int correlationId = msg.readInt();

                // ClientId is a nullable string (int16 length)
                short clientIdLength = msg.readShort();
                String clientId = null;
                if (clientIdLength >= 0) {
                    byte[] clientIdBytes = new byte[clientIdLength];
                    msg.readBytes(clientIdBytes);
                    clientId = new String(clientIdBytes);
                }

                // Reset reader index to keep the full message for forwarding
                msg.readerIndex(readerIndex);

                out.add(new KafkaMessage(msg.retain(), apiKey, apiVersion, correlationId, clientId));
            } catch (Exception e) {
                // Fallback to raw bytes if decoding fails
                msg.readerIndex(readerIndex);
                out.add(msg.retain());
            }
        } else {
            // Responses from broker
            out.add(msg.retain());
        }
    }
}
