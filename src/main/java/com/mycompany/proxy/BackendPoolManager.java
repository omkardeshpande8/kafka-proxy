package com.mycompany.proxy;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.pool.AbstractChannelPoolMap;
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.channel.pool.ChannelPoolMap;
import io.netty.channel.pool.FixedChannelPool;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.ssl.SslContext;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import com.mycompany.proxy.interceptor.KafkaInterceptorChain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BackendPoolManager {
    private static final Logger logger = LoggerFactory.getLogger(BackendPoolManager.class);

    private final ChannelPoolMap<KafkaProxy.BackendTarget, FixedChannelPool> poolMap;
    private final EventLoopGroup workerGroup;
    private final SslContext backendSslContext;
    private final KafkaInterceptorChain interceptorChain;

    public BackendPoolManager(EventLoopGroup workerGroup, SslContext backendSslContext, KafkaInterceptorChain interceptorChain) {
        this.workerGroup = workerGroup;
        this.backendSslContext = backendSslContext;
        this.interceptorChain = interceptorChain;

        this.poolMap = new AbstractChannelPoolMap<KafkaProxy.BackendTarget, FixedChannelPool>() {
            @Override
            protected FixedChannelPool newPool(KafkaProxy.BackendTarget key) {
                Bootstrap b = new Bootstrap()
                        .group(workerGroup)
                        .channel(io.netty.channel.socket.nio.NioSocketChannel.class)
                        .option(ChannelOption.TCP_NODELAY, true)
                        .option(ChannelOption.SO_KEEPALIVE, true)
                        .remoteAddress(key.host(), key.port());

                return new FixedChannelPool(b, new ChannelPoolHandler() {
                    @Override
                    public void channelReleased(Channel ch) throws Exception {
                        logger.debug("Channel released to pool: {}", ch);
                    }

                    @Override
                    public void channelAcquired(Channel ch) throws Exception {
                        logger.debug("Channel acquired from pool: {}", ch);
                    }

                    @Override
                    public void channelCreated(Channel ch) throws Exception {
                        logger.info("New backend channel created for {}: {}", key, ch);
                        if (backendSslContext != null) {
                            io.netty.handler.ssl.SslHandler sslHandler = backendSslContext.newHandler(ch.alloc(), key.host(), key.port());
                            javax.net.ssl.SSLEngine engine = sslHandler.engine();
                            javax.net.ssl.SSLParameters params = engine.getSSLParameters();
                            params.setEndpointIdentificationAlgorithm("HTTPS");
                            engine.setSSLParameters(params);
                            ch.pipeline().addLast("ssl", sslHandler);
                        }
                        ch.pipeline().addLast("frameDecoder", new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4));
                        ch.pipeline().addLast("frameEncoder", new LengthFieldPrepender(4));
                        // BackendHandler will be added dynamically or updated with frontendCtx
                    }
                }, 50); // Max 50 connections per backend
            }
        };
    }

    public Future<Channel> acquire(KafkaProxy.BackendTarget target) {
        return poolMap.get(target).acquire();
    }

    public void release(KafkaProxy.BackendTarget target, Channel channel) {
        poolMap.get(target).release(channel);
    }

    public void close() {
        // poolMap doesn't have a close, but we should ideally close all pools
    }
}
