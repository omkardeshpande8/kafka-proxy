package com.example.proxy;

import com.example.proxy.interceptor.KafkaInterceptorChain;
import com.example.proxy.protocol.KafkaProtocolHandler;
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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class KafkaProxy {

    static final class BackendTarget {
        private final String host;
        private final int port;

        BackendTarget(String host, int port) {
            this.host = host;
            this.port = port;
        }

        String host() {
            return host;
        }

        int port() {
            return port;
        }

        @Override
        public String toString() {
            return host + ":" + port;
        }
    }

    private final int localPort;
    private final KafkaInterceptorChain interceptorChain;
    private final Map<String, BackendTarget> backends = new ConcurrentHashMap<>();
    private final CopyOnWriteArrayList<TopicRoutingRule> topicRoutingRules = new CopyOnWriteArrayList<>();

    private volatile String defaultBackendName;

    public KafkaProxy(int localPort, String remoteHost, int remotePort) {
        this(localPort, remoteHost, remotePort, new KafkaInterceptorChain());
    }

    public KafkaProxy(int localPort, String remoteHost, int remotePort, KafkaInterceptorChain interceptorChain) {
        this.localPort = localPort;
        this.interceptorChain = interceptorChain;
        registerBackend("default", remoteHost, remotePort);
        this.defaultBackendName = "default";
    }

    public synchronized void registerBackend(String name, String host, int port) {
        backends.put(name, new BackendTarget(host, port));
    }

    public synchronized void failover(String newHost, int newPort) {
        System.out.println("[FAILOVER] Switching default backend to " + newHost + ":" + newPort);
        registerBackend(defaultBackendName, newHost, newPort);
    }

    public synchronized void failoverBackend(String backendName, String newHost, int newPort) {
        System.out.println("[FAILOVER] Switching backend " + backendName + " to " + newHost + ":" + newPort);
        registerBackend(backendName, newHost, newPort);
    }

    public synchronized void setDefaultBackend(String backendName) {
        if (!backends.containsKey(backendName)) {
            throw new IllegalArgumentException("Unknown backend: " + backendName);
        }
        this.defaultBackendName = backendName;
    }

    public synchronized void addOrUpdateTopicRoute(String topicRegex, String backendName) {
        if (!backends.containsKey(backendName)) {
            throw new IllegalArgumentException("Unknown backend for route: " + backendName);
        }

        for (TopicRoutingRule rule : topicRoutingRules) {
            if (rule.rawPattern().equals(topicRegex)) {
                rule.setBackendName(backendName);
                System.out.println("[ROUTING] Updated route " + topicRegex + " -> " + backendName);
                return;
            }
        }

        topicRoutingRules.add(new TopicRoutingRule(topicRegex, backendName));
        System.out.println("[ROUTING] Added route " + topicRegex + " -> " + backendName);
    }

    public synchronized void clearTopicRoutes() {
        topicRoutingRules.clear();
    }

    public BackendTarget resolveBackend(String topic) {
        if (topic != null) {
            for (TopicRoutingRule rule : topicRoutingRules) {
                if (rule.matches(topic)) {
                    BackendTarget routeTarget = backends.get(rule.backendName());
                    if (routeTarget != null) {
                        return routeTarget;
                    }
                }
            }
        }

        BackendTarget fallback = backends.get(defaultBackendName);
        if (fallback == null) {
            throw new IllegalStateException("No backend configured for default backend name " + defaultBackendName);
        }
        return fallback;
    }

    public synchronized String getRemoteHost() {
        return resolveBackend(null).host();
    }

    public synchronized int getRemotePort() {
        return resolveBackend(null).port();
    }

    public void run() throws Exception {
        System.out.println(
                "Starting Kafka Proxy on port " + localPort + " with default backend " + resolveBackend(null));

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
        int localPort = Integer.parseInt(getEnv("PROXY_PORT", "9092"));
        String remoteHost = getEnv("BACKEND_HOST", "localhost");
        int remotePort = Integer.parseInt(getEnv("BACKEND_PORT", "9093"));
        String configPath = getEnv("CONFIG_PATH", "proxy.properties");

        KafkaInterceptorChain chain = ProxyConfig.loadInterceptors(configPath);
        KafkaProxy proxy = new KafkaProxy(localPort, remoteHost, remotePort, chain);
        ProxyConfig.applyRoutingConfig(proxy, configPath);

        proxy.run();
    }
}
