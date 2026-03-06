package com.example.proxy;

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
import com.example.proxy.interceptor.KafkaInterceptorChain;
import com.example.proxy.protocol.KafkaMessage;

public class ProxyFrontendHandler extends ChannelInboundHandlerAdapter {

    private final String remoteHost;
    private final int remotePort;

    // The outbound channel to the backend server
    private volatile Channel outboundChannel;
    private final KafkaInterceptorChain interceptorChain;

    public ProxyFrontendHandler(String remoteHost, int remotePort) {
        this(remoteHost, remotePort, new KafkaInterceptorChain());
    }

    public ProxyFrontendHandler(String remoteHost, int remotePort, KafkaInterceptorChain interceptorChain) {
        this.remoteHost = remoteHost;
        this.remotePort = remotePort;
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
                        ch.pipeline().addLast("backendHandler", new ProxyBackendHandler(inboundChannel));
                    }
                })
                .option(ChannelOption.AUTO_READ, false);

        ChannelFuture f = b.connect(remoteHost, remotePort);
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
        Object forwardMsg = msg;
        if (msg instanceof KafkaMessage) {
            KafkaMessage km = (KafkaMessage) msg;
            if (!interceptorChain.onRequest(ctx, km)) {
                // Interceptor blocked the request
                // For now, close the connection to avoid client hanging
                km.release();
                if (outboundChannel != null) {
                    outboundChannel.close();
                }
                ctx.close();
                return;
            }
            // Unwrap KafkaMessage to ByteBuf for forwarding
            forwardMsg = km.payload();
        }

        if (outboundChannel.isActive()) {
            outboundChannel.writeAndFlush(forwardMsg).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) {
                    if (future.isSuccess()) {
                        // was able to flush out data, start to read the next chunk
                        ctx.channel().read();
                    } else {
                        future.channel().close();
                    }
                }
            });
        } else {
            // Outbound channel not active, release message if needed
            if (forwardMsg instanceof io.netty.util.ReferenceCounted) {
                ((io.netty.util.ReferenceCounted) forwardMsg).release();
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
