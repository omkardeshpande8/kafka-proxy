package com.mycompany.proxy;

import com.mycompany.proxy.interceptor.KafkaInterceptorChain;
import com.mycompany.proxy.protocol.KafkaProtocolHandler;
import com.mycompany.proxy.security.ProxySecurityConfig;
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
import io.netty.handler.ssl.SslContext;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaProxy {
    private static final Logger logger = LoggerFactory.getLogger(KafkaProxy.class);

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
    private final SslContext frontendSslContext;
    private final SslContext backendSslContext;
    private BackendPoolManager poolManager;
    private final Map<String, BackendTarget> backends = new ConcurrentHashMap<>();
    private final CopyOnWriteArrayList<TopicRoutingRule> topicRoutingRules = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<GroupRoutingRule> groupRoutingRules = new CopyOnWriteArrayList<>();

    private volatile String defaultBackendName;

    public KafkaProxy(int localPort, String remoteHost, int remotePort) {
        this(localPort, remoteHost, remotePort, new KafkaInterceptorChain(), new ProxySecurityConfig(null, null));
    }

    public KafkaProxy(int localPort, String remoteHost, int remotePort, KafkaInterceptorChain interceptorChain) {
        this(localPort, remoteHost, remotePort, interceptorChain, new ProxySecurityConfig(null, null));
    }

    public KafkaProxy(int localPort,
                      String remoteHost,
                      int remotePort,
                      KafkaInterceptorChain interceptorChain,
                      ProxySecurityConfig securityConfig) {
        this.localPort = localPort;
        this.interceptorChain = interceptorChain;
        this.frontendSslContext = securityConfig.frontendSslContext();
        this.backendSslContext = securityConfig.backendSslContext();
        registerBackend("default", remoteHost, remotePort);
        this.defaultBackendName = "default";
    }

    public synchronized void registerBackend(String name, String host, int port) {
        backends.put(name, new BackendTarget(host, port));
    }

    public synchronized void failover(String newHost, int newPort) {
        logger.info("[FAILOVER] Switching default backend to {}:{}", newHost, newPort);
        registerBackend(defaultBackendName, newHost, newPort);
    }

    public synchronized void failoverBackend(String backendName, String newHost, int newPort) {
        logger.info("[FAILOVER] Switching backend {} to {}:{}", backendName, newHost, newPort);
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
                logger.info("[ROUTING] Updated topic route {} -> {}", topicRegex, backendName);
                return;
            }
        }

        topicRoutingRules.add(new TopicRoutingRule(topicRegex, backendName));
        logger.info("[ROUTING] Added topic route {} -> {}", topicRegex, backendName);
    }

    public synchronized void addOrUpdateGroupRoute(String groupRegex, String backendName) {
        if (!backends.containsKey(backendName)) {
            throw new IllegalArgumentException("Unknown backend for route: " + backendName);
        }

        for (GroupRoutingRule rule : groupRoutingRules) {
            if (rule.rawPattern().equals(groupRegex)) {
                rule.setBackendName(backendName);
                logger.info("[ROUTING] Updated group route {} -> {}", groupRegex, backendName);
                return;
            }
        }

        groupRoutingRules.add(new GroupRoutingRule(groupRegex, backendName));
        logger.info("[ROUTING] Added group route {} -> {}", groupRegex, backendName);
    }

    public synchronized void clearTopicRoutes() {
        topicRoutingRules.clear();
    }

    public synchronized void clearGroupRoutes() {
        groupRoutingRules.clear();
    }

    public BackendTarget resolveBackend(String topic, String groupId) {
        if (groupId != null) {
            for (GroupRoutingRule rule : groupRoutingRules) {
                if (rule.matches(groupId)) {
                    BackendTarget routeTarget = backends.get(rule.backendName());
                    if (routeTarget != null) {
                        return routeTarget;
                    }
                }
            }
        }

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
        return resolveBackend(null, null).host();
    }

    public synchronized int getRemotePort() {
        return resolveBackend(null, null).port();
    }

    public void run() throws Exception {
        startHealthCheckServer();
        logger.info("Starting Kafka Proxy on port {} with default backend {}", localPort, resolveBackend(null, null));

        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        this.poolManager = new BackendPoolManager(workerGroup, backendSslContext, interceptorChain);
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) {
                            if (frontendSslContext != null) {
                                ch.pipeline().addLast("ssl", frontendSslContext.newHandler(ch.alloc()));
                            }
                            ch.pipeline().addLast("frameDecoder", new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4));
                            ch.pipeline().addLast("frameEncoder", new LengthFieldPrepender(4));
                            ch.pipeline().addLast("protocolHandler", new KafkaProtocolHandler());
                            ch.pipeline().addLast("frontendHandler", new ProxyFrontendHandler(KafkaProxy.this, interceptorChain, backendSslContext, poolManager));
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

    private void startHealthCheckServer() {
        try {
            int healthPort = Integer.parseInt(getEnv("HEALTH_PORT", "8080"));
            HttpServer server = HttpServer.create(new InetSocketAddress(healthPort), 0);
            server.createContext("/health", new HttpHandler() {
                @Override
                public void handle(HttpExchange exchange) throws IOException {
                    String response = "OK";
                    exchange.sendResponseHeaders(200, response.length());
                    try (OutputStream os = exchange.getResponseBody()) {
                        os.write(response.getBytes());
                    }
                }
            });
            server.createContext("/metrics", new HttpHandler() {
                @Override
                public void handle(HttpExchange exchange) throws IOException {
                    String response = MetricsRegistry.toPrometheus();
                    exchange.sendResponseHeaders(200, response.length());
                    try (OutputStream os = exchange.getResponseBody()) {
                        os.write(response.getBytes());
                    }
                }
            });
            server.setExecutor(null);
            server.start();
            logger.info("[HEALTH] Health check server started on port {}", healthPort);
        } catch (Exception e) {
            logger.error("[HEALTH] Failed to start health check server: {}", e.getMessage());
        }
    }

    static class GroupRoutingRule {
        private final String pattern;
        private final java.util.regex.Pattern regex;
        private String backendName;

        GroupRoutingRule(String pattern, String backendName) {
            this.pattern = pattern;
            this.regex = java.util.regex.Pattern.compile(pattern);
            this.backendName = backendName;
        }

        String rawPattern() {
            return pattern;
        }

        boolean matches(String groupId) {
            return regex.matcher(groupId).matches();
        }

        String backendName() {
            return backendName;
        }

        void setBackendName(String backendName) {
            this.backendName = backendName;
        }
    }

    public static void main(String[] args) throws Exception {
        int localPort = Integer.parseInt(getEnv("PROXY_PORT", "9092"));
        String remoteHost = getEnv("BACKEND_HOST", "localhost");
        int remotePort = Integer.parseInt(getEnv("BACKEND_PORT", "9093"));
        String configPath = getEnv("CONFIG_PATH", "proxy.properties");

        KafkaInterceptorChain chain = ProxyConfig.loadInterceptors(configPath);
        ProxySecurityConfig securityConfig = ProxyConfig.loadSecurityConfig(configPath);
        KafkaProxy proxy = new KafkaProxy(localPort, remoteHost, remotePort, chain, securityConfig);
        ProxyConfig.applyRoutingConfig(proxy, configPath);

        proxy.run();
    }
}
