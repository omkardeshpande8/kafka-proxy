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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ProxyFrontendHandler extends ChannelInboundHandlerAdapter {

    private final KafkaProxy proxy;
    private final Map<KafkaProxy.BackendTarget, Channel> outboundChannels = new ConcurrentHashMap<>();
    private final KafkaInterceptorChain interceptorChain;

    public ProxyFrontendHandler(KafkaProxy proxy) {
        this(proxy, new KafkaInterceptorChain());
    }

    public ProxyFrontendHandler(KafkaProxy proxy, KafkaInterceptorChain interceptorChain) {
        this.proxy = proxy;
        this.interceptorChain = interceptorChain;
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
                    String topic = TopicExtractor.extractTopic(km);
                    ensureConnectedAndForward(ctx, km.payload(), topic);
                }

                @Override
                public void block() {
                    km.release();
                    closeAllOutboundConnections();
                    ctx.close();
                }
            });
        } else {
            ensureConnectedAndForward(ctx, msg, null);
        }
    }

    private void ensureConnectedAndForward(final ChannelHandlerContext ctx, final Object msg, String topic) {
        KafkaProxy.BackendTarget requestedTarget = proxy.resolveBackend(topic);

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
                        ch.pipeline().addLast("frameDecoder", new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4));
                        ch.pipeline().addLast("frameEncoder", new LengthFieldPrepender(4));
                        ch.pipeline().addLast("backendHandler", new ProxyBackendHandler(inboundChannel, interceptorChain));
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

    static void closeOnFlush(Channel ch) {
        if (ch.isActive()) {
            ch.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
        }
    }
}
