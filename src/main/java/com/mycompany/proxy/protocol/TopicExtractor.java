package com.mycompany.proxy.protocol;

import io.netty.buffer.ByteBuf;
import org.apache.kafka.common.protocol.ApiKeys;

import java.nio.charset.StandardCharsets;

public final class TopicExtractor {

    private TopicExtractor() {
    }

    public static String extractTopic(KafkaMessage message) {
        if (message.apiKey() == ApiKeys.PRODUCE.id) {
            return extractProduceTopic(message);
        }

        if (message.apiKey() == ApiKeys.FETCH.id) {
            return extractFetchTopic(message);
        }

        return null;
    }

    private static String extractProduceTopic(KafkaMessage message) {
        ByteBuf payload = message.body();
        int readerIndex = payload.readerIndex();
        try {
            if (message.apiVersion() >= 3) {
                short transIdLen = payload.readShort();
                if (transIdLen > 0) {
                    payload.skipBytes(transIdLen);
                }
            }
            payload.readShort();
            payload.readInt();
            int topicsLen = payload.readInt();
            if (topicsLen <= 0) {
                return null;
            }
            return readTopic(payload);
        } catch (Exception e) {
            return null;
        } finally {
            payload.readerIndex(readerIndex);
        }
    }

    private static String extractFetchTopic(KafkaMessage message) {
        ByteBuf payload = message.body();
        int readerIndex = payload.readerIndex();
        try {
            payload.readInt();
            payload.readInt();
            payload.readInt();

            if (message.apiVersion() >= 3) {
                payload.readInt();
            }
            if (message.apiVersion() >= 4) {
                payload.readByte();
            }
            if (message.apiVersion() >= 7) {
                payload.readInt();
                payload.readInt();
            }
            if (message.apiVersion() >= 11) {
                short rackIdLength = payload.readShort();
                if (rackIdLength > 0) {
                    payload.skipBytes(rackIdLength);
                }
            }

            int topicsLen = payload.readInt();
            if (topicsLen <= 0) {
                return null;
            }
            return readTopic(payload);
        } catch (Exception e) {
            return null;
        } finally {
            payload.readerIndex(readerIndex);
        }
    }

    private static String readTopic(ByteBuf payload) {
        short topicNameLen = payload.readShort();
        if (topicNameLen < 0 || payload.readableBytes() < topicNameLen) {
            return null;
        }
        byte[] topicBytes = new byte[topicNameLen];
        payload.readBytes(topicBytes);
        return new String(topicBytes, StandardCharsets.UTF_8);
    }
}
