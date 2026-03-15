package com.mycompany.proxy.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCounted;

public class KafkaMessage implements ReferenceCounted {
    private final ByteBuf fullMessage;
    private final short apiKey;
    private final short apiVersion;
    private final int correlationId;
    private final String clientId;
    private final int headerSize;

    public KafkaMessage(ByteBuf fullMessage, short apiKey, short apiVersion, int correlationId, String clientId, int headerSize) {
        this.fullMessage = fullMessage;
        this.apiKey = apiKey;
        this.apiVersion = apiVersion;
        this.correlationId = correlationId;
        this.clientId = clientId;
        this.headerSize = headerSize;
    }

    public ByteBuf payload() {
        // Return the full message starting from the beginning (including header)
        return fullMessage.setIndex(0, fullMessage.writerIndex());
    }

    public ByteBuf body() {
        // Just the body after the header
        return fullMessage.setIndex(headerSize, fullMessage.writerIndex());
    }

    public short apiKey() {
        return apiKey;
    }

    public short apiVersion() {
        return apiVersion;
    }

    public int correlationId() {
        return correlationId;
    }

    public String clientId() {
        return clientId;
    }

    @Override
    public int refCnt() {
        return fullMessage.refCnt();
    }

    @Override
    public KafkaMessage retain() {
        fullMessage.retain();
        return this;
    }

    @Override
    public KafkaMessage retain(int increment) {
        fullMessage.retain(increment);
        return this;
    }

    @Override
    public KafkaMessage touch() {
        fullMessage.touch();
        return this;
    }

    @Override
    public KafkaMessage touch(Object hint) {
        fullMessage.touch(hint);
        return this;
    }

    @Override
    public boolean release() {
        return fullMessage.release();
    }

    @Override
    public boolean release(int decrement) {
        return fullMessage.release(decrement);
    }

    @Override
    public String toString() {
        return "KafkaMessage{" +
                "apiKey=" + apiKey +
                ", apiVersion=" + apiVersion +
                ", correlationId=" + correlationId +
                ", clientId='" + clientId + '\'' +
                '}';
    }
}
