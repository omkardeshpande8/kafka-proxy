package com.mycompany.proxy.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCounted;

public class KafkaMessage implements ReferenceCounted {
    private ByteBuf fullMessage;
    private final short apiKey;
    private final short apiVersion;
    private final int correlationId;
    private final String clientId;
    private int headerSize;

    public KafkaMessage(ByteBuf fullMessage, short apiKey, short apiVersion, int correlationId, String clientId, int headerSize) {
        this.fullMessage = fullMessage;
        this.apiKey = apiKey;
        this.apiVersion = apiVersion;
        this.correlationId = correlationId;
        this.clientId = clientId;
        this.headerSize = headerSize;
    }

    public void replacePayload(ByteBuf newFullMessage, int newHeaderSize) {
        if (this.fullMessage != null) {
            this.fullMessage.release();
        }
        this.fullMessage = newFullMessage;
        this.headerSize = newHeaderSize;
        // Maintain invariant: fullMessage readerIndex is at the start of the body
        this.fullMessage.readerIndex(newHeaderSize);
    }

    public ByteBuf payload() {
        // Return the full message (header + body)
        return fullMessage.duplicate().readerIndex(fullMessage.readerIndex() - headerSize);
    }

    public ByteBuf body() {
        // Return the body only
        return fullMessage.slice();
    }

    public int headerSize() {
        return headerSize;
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
