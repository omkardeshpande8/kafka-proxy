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
    private final KafkaProxy.BackendTarget target;

    public ProxyBackendHandler(ProxyFrontendHandler frontendHandler, ChannelHandlerContext frontendCtx, KafkaInterceptorChain interceptorChain, KafkaProxy.BackendTarget target) {
        this.frontendHandler = frontendHandler;
        this.frontendCtx = frontendCtx;
        this.interceptorChain = interceptorChain;
        this.target = target;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        ctx.read();
    }

    @Override
    public void channelRead(final ChannelHandlerContext ctx, Object msg) {
        interceptorChain.onResponse(frontendCtx, msg);
        frontendHandler.handleBackendResponse(frontendCtx, msg, target);
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
