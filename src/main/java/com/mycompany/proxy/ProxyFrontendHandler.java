package com.mycompany.proxy;

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
import com.mycompany.proxy.interceptor.KafkaInterceptorChain;
import com.mycompany.proxy.protocol.KafkaMessage;

public class ProxyFrontendHandler extends ChannelInboundHandlerAdapter {

    private final KafkaProxy proxy;

    // The outbound channel to the backend server
    private volatile Channel outboundChannel;
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
        final Channel inboundChannel = ctx.channel();

        // Start the connection attempt.
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

        ChannelFuture f = b.connect(proxy.getRemoteHost(), proxy.getRemotePort());
        outboundChannel = f.channel();
        f.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) {
                if (future.isSuccess()) {
                    // Connection complete start to read first data
                    inboundChannel.read();
                } else {
                    // Close the connection if the connection attempt has failed.
                    inboundChannel.close();
                }
            }
        });
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof KafkaMessage) {
            KafkaMessage km = (KafkaMessage) msg;
            interceptorChain.onRequest(ctx, km, new KafkaInterceptorChain.Callback() {
                @Override
                public void proceed() {
                    forward(ctx, km.payload());
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
            forward(ctx, msg);
        }
    }

    private void forward(final ChannelHandlerContext ctx, Object msg) {
        if (outboundChannel.isActive()) {
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

    /**
     * Closes the specified channel after all queued write requests are flushed.
     */
    static void closeOnFlush(Channel ch) {
        if (ch.isActive()) {
            ch.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
        }
    }
}
