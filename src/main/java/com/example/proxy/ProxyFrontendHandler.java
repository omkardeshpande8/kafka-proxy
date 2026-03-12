package com.example.proxy;

import com.example.proxy.interceptor.KafkaInterceptorChain;
import com.example.proxy.protocol.KafkaMessage;
import com.example.proxy.protocol.TopicExtractor;
import io.netty.bootstrap.Bootstrap;
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

public class ProxyFrontendHandler extends ChannelInboundHandlerAdapter {

    private final KafkaProxy proxy;
    private volatile Channel outboundChannel;
    private volatile KafkaProxy.BackendTarget pinnedBackendTarget;
    private final KafkaInterceptorChain interceptorChain;
    private final SslContext backendSslContext;

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
        // We defer backend connect until first message so we can route by topic.
        ctx.channel().read();
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof KafkaMessage) {
            KafkaMessage km = (KafkaMessage) msg;
            interceptorChain.onRequest(ctx, km, new KafkaInterceptorChain.Callback() {
                @Override
                public void proceed() {
                    ensureConnectedAndForward(ctx, km.payload(), TopicExtractor.extractTopic(km));
                }

                @Override
                public void block() {
                    km.release();
                    if (outboundChannel != null) {
                        outboundChannel.close();
                    }
                    ctx.close();
                }
            });
        } else {
            ensureConnectedAndForward(ctx, msg, null);
        }
    }

    private void ensureConnectedAndForward(final ChannelHandlerContext ctx, final Object msg, String topic) {
        KafkaProxy.BackendTarget requestedTarget = proxy.resolveBackend(topic);

        if (pinnedBackendTarget != null && !isSameTarget(pinnedBackendTarget, requestedTarget)) {
            System.err.println("[ROUTING] Rejecting request because connection is pinned to "
                    + pinnedBackendTarget + " but topic resolved to " + requestedTarget);
            if (msg instanceof io.netty.util.ReferenceCounted) {
                ((io.netty.util.ReferenceCounted) msg).release();
            }
            ctx.close();
            return;
        }

        if (outboundChannel != null && outboundChannel.isActive()) {
            forward(ctx, msg);
            return;
        }

        connectForTarget(ctx, requestedTarget, new Runnable() {
            @Override
            public void run() {
                forward(ctx, msg);
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
                            ch.pipeline().addLast("ssl", backendSslContext.newHandler(ch.alloc(), target.host(), target.port()));
                        }
                        ch.pipeline().addLast("frameDecoder", new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4));
                        ch.pipeline().addLast("frameEncoder", new LengthFieldPrepender(4));
                        ch.pipeline().addLast("backendHandler", new ProxyBackendHandler(inboundChannel, interceptorChain));
                    }
                })
                .option(ChannelOption.AUTO_READ, false);

        ChannelFuture f = b.connect(target.host(), target.port());
        outboundChannel = f.channel();
        f.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) {
                if (future.isSuccess()) {
                    pinnedBackendTarget = target;
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


    private boolean isSameTarget(KafkaProxy.BackendTarget left, KafkaProxy.BackendTarget right) {
        return left.host().equals(right.host()) && left.port() == right.port();
    }

    private void forward(final ChannelHandlerContext ctx, Object msg) {
        if (outboundChannel != null && outboundChannel.isActive()) {
            outboundChannel.writeAndFlush(msg).addListener(new ChannelFutureListener() {
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
        if (outboundChannel != null) {
            closeOnFlush(outboundChannel);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        closeOnFlush(ctx.channel());
    }

    static void closeOnFlush(Channel ch) {
        if (ch.isActive()) {
            ch.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
        }
    }
}
