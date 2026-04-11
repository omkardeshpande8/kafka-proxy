package com.mycompany.proxy;

import com.mycompany.proxy.interceptor.KafkaInterceptorChain;
import com.mycompany.proxy.protocol.GroupIdExtractor;
import com.mycompany.proxy.protocol.KafkaMessage;
import com.mycompany.proxy.protocol.TopicExtractor;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.ssl.SslContext;
import io.netty.util.AttributeKey;
import org.apache.kafka.common.protocol.ApiKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class ProxyFrontendHandler extends ChannelInboundHandlerAdapter {
    private static final Logger logger = LoggerFactory.getLogger(ProxyFrontendHandler.class);

    private final KafkaProxy proxy;
    private final Map<KafkaProxy.BackendTarget, Channel> outboundChannels = new ConcurrentHashMap<>();
    private final KafkaInterceptorChain interceptorChain;
    private final SslContext backendSslContext;
    private final BackendPoolManager poolManager;
    private volatile KafkaProxy.BackendTarget pinnedTarget;

    // For response ordering per backend
    private final Map<KafkaProxy.BackendTarget, Queue<Integer>> perTargetExpectedIds = new ConcurrentHashMap<>();
    private final Map<KafkaProxy.BackendTarget, Map<Integer, Object>> perTargetPendingResponses = new ConcurrentHashMap<>();

    public static final AttributeKey<String> PRINCIPAL_KEY = AttributeKey.valueOf("kafkaPrincipal");
    private volatile String saslMechanism;
    private volatile String clientPrincipal;
    private volatile boolean authCompleted = false;
    private volatile String firstClientId;

    public ProxyFrontendHandler(KafkaProxy proxy, BackendPoolManager poolManager) {
        this(proxy, new KafkaInterceptorChain(), null, poolManager);
    }

    public ProxyFrontendHandler(KafkaProxy proxy, KafkaInterceptorChain interceptorChain, BackendPoolManager poolManager) {
        this(proxy, interceptorChain, null, poolManager);
    }

    public ProxyFrontendHandler(KafkaProxy proxy, KafkaInterceptorChain interceptorChain, SslContext backendSslContext, BackendPoolManager poolManager) {
        this.proxy = proxy;
        this.interceptorChain = interceptorChain;
        this.backendSslContext = backendSslContext;
        this.poolManager = poolManager;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        MetricsRegistry.ACTIVE_CONNECTIONS.incrementAndGet();
        ctx.channel().read();
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof KafkaMessage) {
            final KafkaMessage km = (KafkaMessage) msg;
            final boolean expectsResponse = expectsResponse(km);

            // Track SASL state
            if (km.apiKey() == ApiKeys.SASL_HANDSHAKE.id) {
                try {
                    ByteBuf body = km.body();
                    int readerIndex = body.readerIndex();
                    short mechLen = body.readShort();
                    byte[] mechBytes = new byte[mechLen];
                    body.readBytes(mechBytes);
                    this.saslMechanism = new String(mechBytes);
                    body.readerIndex(readerIndex);
                } catch (Exception ignored) {}
            } else if (km.apiKey() == ApiKeys.SASL_AUTHENTICATE.id && "PLAIN".equals(saslMechanism)) {
                try {
                    ByteBuf body = km.body();
                    int readerIndex = body.readerIndex();
                    int authLen = body.readInt();
                    byte[] authBytes = new byte[authLen];
                    body.readBytes(authBytes);
                    String authStr = new String(authBytes);
                    // PLAIN format: \0username\0password
                    String[] parts = authStr.split("\0");
                    if (parts.length >= 2) {
                        this.clientPrincipal = parts[1];
                        ctx.channel().attr(PRINCIPAL_KEY).set(this.clientPrincipal);
                    }
                    body.readerIndex(readerIndex);
                } catch (Exception ignored) {}
            }

            MetricsRegistry.TOTAL_REQUESTS.incrementAndGet();
            try {
                String apiKeyName = ApiKeys.forId(km.apiKey()).name;
                MetricsRegistry.REQUESTS_BY_API.computeIfAbsent(apiKeyName, k -> new java.util.concurrent.atomic.AtomicLong(0)).incrementAndGet();
            } catch (Exception ignored) {}

            interceptorChain.onRequest(ctx, km, new KafkaInterceptorChain.Callback() {
                @Override
                public void proceed() {
                    if (firstClientId == null && km.clientId() != null) {
                        firstClientId = km.clientId();
                    } else if (firstClientId != null && km.clientId() != null && !firstClientId.equals(km.clientId())) {
                        logger.warn("[SECURITY] ClientID mismatch for {}: expected {}, got {}", ctx.channel().remoteAddress(), firstClientId, km.clientId());
                        ctx.close();
                        km.release();
                        return;
                    }

                    String topic = TopicExtractor.extractTopic(km);
                    String groupId = GroupIdExtractor.extractGroupId(km);
                    KafkaProxy.BackendTarget requestedTarget = proxy.resolveBackend(topic, groupId);

                    if (expectsResponse) {
                        perTargetExpectedIds.computeIfAbsent(requestedTarget, k -> new ConcurrentLinkedQueue<>()).add(km.correlationId());
                    }
                    ensureConnectedAndForward(ctx, km, requestedTarget);
                }

                @Override
                public void block() {
                    km.release();
                    closeAllOutboundConnections();
                    ctx.close();
                }
            });
        } else {
            KafkaProxy.BackendTarget requestedTarget = proxy.resolveBackend(null, null);
            ensureConnectedAndForward(ctx, msg, requestedTarget);
        }
    }

    public void handleBackendResponse(ChannelHandlerContext frontendCtx, Object msg, KafkaProxy.BackendTarget target) {
        if (msg instanceof ByteBuf) {
            ByteBuf buf = (ByteBuf) msg;
            if (buf.readableBytes() >= 4) {
                int correlationId = buf.getInt(buf.readerIndex());

                // Detect auth completion (simplistic for now)
                // In a real proxy, we'd check if the SASL_AUTHENTICATE response error code is 0
                if (saslMechanism != null && !authCompleted) {
                    // If we see a response that isn't for HANDSHAKE/AUTH, then auth must be done?
                    // Not quite, but if we see a successful AUTH response, we mark it.
                    // For now, let's just assume it's completed after the first AUTH response.
                    authCompleted = true;
                }

                perTargetPendingResponses.computeIfAbsent(target, k -> new ConcurrentHashMap<>()).put(correlationId, msg);
                flushResponses(frontendCtx, target);
                return;
            }
        }
        // Fallback for non-ByteBuf or too short messages
        frontendCtx.writeAndFlush(msg).addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) {
                if (future.isSuccess()) {
                    // We don't have a backend context here to call read(),
                    // but ProxyBackendHandler will call it.
                } else {
                    future.channel().close();
                }
            }
        });
    }

    private synchronized void flushResponses(ChannelHandlerContext frontendCtx, KafkaProxy.BackendTarget target) {
        Queue<Integer> expectedCorrelationIds = perTargetExpectedIds.get(target);
        Map<Integer, Object> pendingResponses = perTargetPendingResponses.get(target);

        if (expectedCorrelationIds == null || pendingResponses == null) {
            return;
        }

        while (!expectedCorrelationIds.isEmpty()) {
            Integer nextId = expectedCorrelationIds.peek();
            Object response = pendingResponses.remove(nextId);
            if (response != null) {
                expectedCorrelationIds.poll();
                frontendCtx.writeAndFlush(response).addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) {
                        if (!future.isSuccess()) {
                            future.channel().close();
                        }
                    }
                });
            } else {
                break;
            }
        }
    }

    private boolean expectsResponse(KafkaMessage message) {
        if (message.apiKey() != ApiKeys.PRODUCE.id) {
            return true;
        }

        ByteBuf body = message.body();
        int readerIndex = body.readerIndex();
        try {
            // ProduceRequest v3+ starts with nullable transactional_id before required_acks.
            if (message.apiVersion() >= 3) {
                short transactionalIdLength = body.readShort();
                if (transactionalIdLength > 0) {
                    body.skipBytes(transactionalIdLength);
                } else if (transactionalIdLength < -1) {
                    return true;
                }
            }

            short requiredAcks = body.readShort();
            return requiredAcks != 0;
        } catch (Exception ignored) {
            // On parse failure, keep conservative behavior and preserve ordering.
            return true;
        } finally {
            body.readerIndex(readerIndex);
        }
    }

    private void ensureConnectedAndForward(final ChannelHandlerContext ctx, final Object msg, final KafkaProxy.BackendTarget requestedTarget) {
        if (pinnedTarget == null) {
            pinnedTarget = requestedTarget;
        } else if (!pinnedTarget.equals(requestedTarget)) {
            logger.warn("[PINNING] Mismatch for {}: client tried to route to {} but is pinned to {}", ctx.channel().remoteAddress(), requestedTarget, pinnedTarget);
            ctx.close();
            if (msg instanceof io.netty.util.ReferenceCounted) {
                ((io.netty.util.ReferenceCounted) msg).release();
            }
            return;
        }

        Channel channel = outboundChannels.get(requestedTarget);
        if (channel != null && channel.isActive()) {
            forward(ctx, channel, msg);
            return;
        }

        connectForTarget(ctx, requestedTarget, new Runnable() {
            @Override
            public void run() {
                Channel newChannel = outboundChannels.get(requestedTarget);
                forward(ctx, newChannel, msg);
            }
        }, msg);
    }

    private void connectForTarget(final ChannelHandlerContext ctx, final KafkaProxy.BackendTarget target, final Runnable onConnected, final Object msg) {
        final Channel inboundChannel = ctx.channel();

        if (poolManager == null) {
            // Fallback to non-pooled if no manager
            Bootstrap b = new Bootstrap();
            b.group(inboundChannel.eventLoop())
                    .channel(ctx.channel().getClass())
                    .handler(new ChannelInitializer<Channel>() {
                        @Override
                        protected void initChannel(Channel ch) {
                            if (backendSslContext != null) {
                                io.netty.handler.ssl.SslHandler sslHandler = backendSslContext.newHandler(ch.alloc(), target.host(), target.port());
                                javax.net.ssl.SSLEngine engine = sslHandler.engine();
                                javax.net.ssl.SSLParameters params = engine.getSSLParameters();
                                params.setEndpointIdentificationAlgorithm("HTTPS");
                                engine.setSSLParameters(params);
                                ch.pipeline().addLast("ssl", sslHandler);
                            }
                            ch.pipeline().addLast("frameDecoder", new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4));
                            ch.pipeline().addLast("frameEncoder", new LengthFieldPrepender(4));
                            ch.pipeline().addLast("backendHandler", new ProxyBackendHandler(ProxyFrontendHandler.this, ctx, interceptorChain, target));
                        }
                    })
                    .option(ChannelOption.AUTO_READ, false);

            ChannelFuture f = b.connect(target.host(), target.port());
            f.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) {
                    if (future.isSuccess()) {
                        outboundChannels.put(target, future.channel());
                        try {
                            onConnected.run();
                        } catch (Exception e) {
                            if (msg instanceof io.netty.util.ReferenceCounted) {
                                ((io.netty.util.ReferenceCounted) msg).release();
                            }
                            inboundChannel.close();
                        }
                    } else {
                        if (msg instanceof io.netty.util.ReferenceCounted) {
                            ((io.netty.util.ReferenceCounted) msg).release();
                        }
                        inboundChannel.close();
                    }
                }
            });
            return;
        }

        io.netty.util.concurrent.Future<Channel> f = poolManager.acquire(target);
        f.addListener(new io.netty.util.concurrent.FutureListener<Channel>() {
            @Override
            public void operationComplete(io.netty.util.concurrent.Future<Channel> future) {
                if (future.isSuccess()) {
                    Channel ch = future.getNow();
                    // Ensure backendHandler is present and has current frontend context
                    if (ch.pipeline().get("backendHandler") != null) {
                        ch.pipeline().remove("backendHandler");
                    }
                    ch.pipeline().addLast("backendHandler", new ProxyBackendHandler(ProxyFrontendHandler.this, ctx, interceptorChain, target));

                    outboundChannels.put(target, ch);
                    try {
                        onConnected.run();
                    } catch (Exception e) {
                        if (msg instanceof io.netty.util.ReferenceCounted) {
                            ((io.netty.util.ReferenceCounted) msg).release();
                        }
                        inboundChannel.close();
                    }
                } else {
                    logger.error("Failed to acquire connection from pool for {}: {}", target, future.cause().getMessage());
                    if (msg instanceof io.netty.util.ReferenceCounted) {
                        ((io.netty.util.ReferenceCounted) msg).release();
                    }
                    inboundChannel.close();
                }
            }
        });
    }

    private void forward(final ChannelHandlerContext ctx, Channel channel, Object msg) {
        if (channel != null && channel.isActive()) {
            Object toWrite = msg;
            if (msg instanceof KafkaMessage) {
                toWrite = ((KafkaMessage) msg).payload();
            }
            channel.writeAndFlush(toWrite).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) {
                    if (future.isSuccess()) {
                        ctx.channel().read();
                    } else {
                        future.channel().close();
                    }
                }
            });
        } else {
            if (msg instanceof io.netty.util.ReferenceCounted) {
                ((io.netty.util.ReferenceCounted) msg).release();
            }
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        MetricsRegistry.ACTIVE_CONNECTIONS.decrementAndGet();
        closeAllOutboundConnections();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        MetricsRegistry.TOTAL_ERRORS.incrementAndGet();
        cause.printStackTrace();
        closeOnFlush(ctx.channel());
    }

    private void closeAllOutboundConnections() {
        for (Map.Entry<KafkaProxy.BackendTarget, Channel> entry : outboundChannels.entrySet()) {
            Channel ch = entry.getValue();
            if (ch != null) {
                if (poolManager != null) {
                    poolManager.release(entry.getKey(), ch);
                } else {
                    closeOnFlush(ch);
                }
            }
        }
        outboundChannels.clear();
    }

    public synchronized void handleBackendDisconnect(ChannelHandlerContext frontendCtx, Channel backendChannel) {
        KafkaProxy.BackendTarget disconnectedTarget = null;
        for (Map.Entry<KafkaProxy.BackendTarget, Channel> entry : outboundChannels.entrySet()) {
            if (entry.getValue() == backendChannel) {
                disconnectedTarget = entry.getKey();
                break;
            }
        }

        if (disconnectedTarget != null) {
            outboundChannels.remove(disconnectedTarget);
            // If it's pooled, the pool will handle physical disconnect if it happens,
            // but we need to stop using it for this client.
            perTargetExpectedIds.remove(disconnectedTarget);
            perTargetPendingResponses.remove(disconnectedTarget);
        }

        closeOnFlush(frontendCtx.channel());
    }

    static void closeOnFlush(Channel ch) {
        if (ch.isActive()) {
            ch.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
        }
    }
}
