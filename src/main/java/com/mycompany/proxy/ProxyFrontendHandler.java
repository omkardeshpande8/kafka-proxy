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
import org.apache.kafka.common.protocol.ApiKeys;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class ProxyFrontendHandler extends ChannelInboundHandlerAdapter {

    private final KafkaProxy proxy;
    private final Map<KafkaProxy.BackendTarget, Channel> outboundChannels = new ConcurrentHashMap<>();
    private final KafkaInterceptorChain interceptorChain;
    private final SslContext backendSslContext;

    // For response ordering
    private final Queue<Integer> expectedCorrelationIds = new ConcurrentLinkedQueue<>();
    private final Map<Integer, Object> pendingResponses = new ConcurrentHashMap<>();

    public ProxyFrontendHandler(KafkaProxy proxy) {
        this(proxy, new KafkaInterceptorChain(), null);
    }

    public ProxyFrontendHandler(KafkaProxy proxy, KafkaInterceptorChain interceptorChain) {
        this(proxy, interceptorChain, null);
    }

    public ProxyFrontendHandler(KafkaProxy proxy, KafkaInterceptorChain interceptorChain, SslContext backendSslContext) {
        this.proxy = proxy;
        this.interceptorChain = interceptorChain;
        this.backendSslContext = backendSslContext;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        ctx.channel().read();
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof KafkaMessage) {
            final KafkaMessage km = (KafkaMessage) msg;
            final boolean expectsResponse = expectsResponse(km);
            if (expectsResponse) {
                expectedCorrelationIds.add(km.correlationId());
            }

            interceptorChain.onRequest(ctx, km, new KafkaInterceptorChain.Callback() {
                @Override
                public void proceed() {
                    String topic = TopicExtractor.extractTopic(km);
                    String groupId = GroupIdExtractor.extractGroupId(km);
                    ensureConnectedAndForward(ctx, km.payload(), topic, groupId);
                }

                @Override
                public void block() {
                    if (expectsResponse) {
                        expectedCorrelationIds.remove(km.correlationId());
                    }
                    km.release();
                    closeAllOutboundConnections();
                    ctx.close();
                }
            });
        } else {
            ensureConnectedAndForward(ctx, msg, null, null);
        }
    }

    public void handleBackendResponse(ChannelHandlerContext frontendCtx, Object msg) {
        if (msg instanceof ByteBuf) {
            ByteBuf buf = (ByteBuf) msg;
            if (buf.readableBytes() >= 4) {
                int correlationId = buf.getInt(buf.readerIndex());
                pendingResponses.put(correlationId, msg);
                flushResponses(frontendCtx);
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

    private synchronized void flushResponses(ChannelHandlerContext frontendCtx) {
        while (!expectedCorrelationIds.isEmpty()) {
            Integer nextId = expectedCorrelationIds.peek();
            if (pendingResponses.containsKey(nextId)) {
                expectedCorrelationIds.poll();
                Object response = pendingResponses.remove(nextId);
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

    private void ensureConnectedAndForward(final ChannelHandlerContext ctx, final Object msg, String topic, String groupId) {
        KafkaProxy.BackendTarget requestedTarget = proxy.resolveBackend(topic, groupId);

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
                        ch.pipeline().addLast("backendHandler", new ProxyBackendHandler(ProxyFrontendHandler.this, ctx, interceptorChain));
                    }
                })
                .option(ChannelOption.AUTO_READ, false);

        ChannelFuture f = b.connect(target.host(), target.port());
        f.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) {
                if (future.isSuccess()) {
                    outboundChannels.put(target, future.channel());
                    onConnected.run();
                } else {
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
            channel.writeAndFlush(msg).addListener(new ChannelFutureListener() {
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
        closeAllOutboundConnections();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        closeOnFlush(ctx.channel());
    }

    private void closeAllOutboundConnections() {
        for (Channel ch : outboundChannels.values()) {
            if (ch != null) {
                closeOnFlush(ch);
            }
        }
        outboundChannels.clear();
    }

    public synchronized void handleBackendDisconnect(ChannelHandlerContext frontendCtx, Channel backendChannel) {
        outboundChannels.entrySet().removeIf(entry -> entry.getValue() == backendChannel);
        expectedCorrelationIds.clear();
        pendingResponses.clear();
        closeOnFlush(frontendCtx.channel());
    }

    static void closeOnFlush(Channel ch) {
        if (ch.isActive()) {
            ch.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
        }
    }
}
