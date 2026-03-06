package com.example.proxy;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import com.example.proxy.protocol.KafkaProtocolHandler;
import com.example.proxy.interceptor.KafkaInterceptorChain;

public class KafkaProxy {

    private final int localPort;
    private String remoteHost;
    private int remotePort;
    private final KafkaInterceptorChain interceptorChain;

    public KafkaProxy(int localPort, String remoteHost, int remotePort) {
        this(localPort, remoteHost, remotePort, new KafkaInterceptorChain());
    }

    public KafkaProxy(int localPort, String remoteHost, int remotePort, KafkaInterceptorChain interceptorChain) {
        this.localPort = localPort;
        this.remoteHost = remoteHost;
        this.remotePort = remotePort;
        this.interceptorChain = interceptorChain;
    }

    public synchronized void failover(String newHost, int newPort) {
        System.out.println("[FAILOVER] Switching backend to " + newHost + ":" + newPort);
        this.remoteHost = newHost;
        this.remotePort = newPort;
    }

    public synchronized String getRemoteHost() {
        return remoteHost;
    }

    public synchronized int getRemotePort() {
        return remotePort;
    }

    public void run() throws Exception {
        System.out.println(
                "Starting Kafka Proxy on port " + localPort + " forwarding to " + getRemoteHost() + ":" + getRemotePort());

        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast("frameDecoder", new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4));
                            ch.pipeline().addLast("frameEncoder", new LengthFieldPrepender(4));
                            ch.pipeline().addLast("protocolHandler", new KafkaProtocolHandler());
                            ch.pipeline().addLast("frontendHandler", new ProxyFrontendHandler(KafkaProxy.this, interceptorChain));
                        }
                    })
                    .childOption(ChannelOption.AUTO_READ, false)
                    .bind(localPort).sync().channel().closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    private static String getEnv(String name, String defaultValue) {
        String value = System.getenv(name);
        return value != null ? value : defaultValue;
    }

    public static void main(String[] args) throws Exception {
        // Load configuration from environment variables with defaults
        int localPort = Integer.parseInt(getEnv("PROXY_PORT", "9092"));
        String remoteHost = getEnv("BACKEND_HOST", "localhost");
        int remotePort = Integer.parseInt(getEnv("BACKEND_PORT", "9093"));
        String configPath = getEnv("CONFIG_PATH", "proxy.properties");

        KafkaInterceptorChain chain = ProxyConfig.loadInterceptors(configPath);

        new KafkaProxy(localPort, remoteHost, remotePort, chain).run();
    }
}
