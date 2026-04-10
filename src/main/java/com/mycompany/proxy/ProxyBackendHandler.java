package com.mycompany.proxy;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import com.mycompany.proxy.interceptor.KafkaInterceptorChain;

public class ProxyBackendHandler extends ChannelInboundHandlerAdapter {

    private final ProxyFrontendHandler frontendHandler;
    private final ChannelHandlerContext frontendCtx;
    private final KafkaInterceptorChain interceptorChain;

    public ProxyBackendHandler(ProxyFrontendHandler frontendHandler, ChannelHandlerContext frontendCtx, KafkaInterceptorChain interceptorChain) {
        this.frontendHandler = frontendHandler;
        this.frontendCtx = frontendCtx;
        this.interceptorChain = interceptorChain;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        ctx.read();
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, Object msg) {
        interceptorChain.onResponse(frontendCtx, msg);
        frontendHandler.handleBackendResponse(frontendCtx, msg);
        ctx.read();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        frontendHandler.handleBackendDisconnect(frontendCtx, ctx.channel());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ProxyFrontendHandler.closeOnFlush(ctx.channel());
    }
}
