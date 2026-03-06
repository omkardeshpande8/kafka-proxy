package com.example.proxy.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCounted;

public class KafkaMessage implements ReferenceCounted {
    private final ByteBuf payload;
    private final short apiKey;
    private final short apiVersion;
    private final int correlationId;
    private final String clientId;

    public KafkaMessage(ByteBuf payload, short apiKey, short apiVersion, int correlationId, String clientId) {
        this.payload = payload;
        this.apiKey = apiKey;
        this.apiVersion = apiVersion;
        this.correlationId = correlationId;
        this.clientId = clientId;
    }

    public ByteBuf payload() {
        return payload;
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
        return payload.refCnt();
    }

    @Override
    public KafkaMessage retain() {
        payload.retain();
        return this;
    }

    @Override
    public KafkaMessage retain(int increment) {
        payload.retain(increment);
        return this;
    }

    @Override
    public KafkaMessage touch() {
        payload.touch();
        return this;
    }

    @Override
    public KafkaMessage touch(Object hint) {
        payload.touch(hint);
        return this;
    }

    @Override
    public boolean release() {
        return payload.release();
    }

    @Override
    public boolean release(int decrement) {
        return payload.release(decrement);
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
