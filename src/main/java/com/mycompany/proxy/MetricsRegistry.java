package com.mycompany.proxy;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MetricsRegistry {
    public static final AtomicInteger ACTIVE_CONNECTIONS = new AtomicInteger(0);
    public static final AtomicLong TOTAL_REQUESTS = new AtomicLong(0);
    public static final AtomicLong TOTAL_ERRORS = new AtomicLong(0);
    public static final Map<String, AtomicLong> REQUESTS_BY_API = new ConcurrentHashMap<>();

    public static String toPrometheus() {
        StringBuilder sb = new StringBuilder();
        sb.append("# HELP proxy_active_connections Number of active client connections\n");
        sb.append("# TYPE proxy_active_connections gauge\n");
        sb.append("proxy_active_connections ").append(ACTIVE_CONNECTIONS.get()).append("\n");

        sb.append("# HELP proxy_requests_total Total number of Kafka requests processed\n");
        sb.append("# TYPE proxy_requests_total counter\n");
        sb.append("proxy_requests_total ").append(TOTAL_REQUESTS.get()).append("\n");

        sb.append("# HELP proxy_errors_total Total number of proxy errors\n");
        sb.append("# TYPE proxy_errors_total counter\n");
        sb.append("proxy_errors_total ").append(TOTAL_ERRORS.get()).append("\n");

        for (Map.Entry<String, AtomicLong> entry : REQUESTS_BY_API.entrySet()) {
            sb.append("proxy_requests_by_api_total{api=\"").append(entry.getKey()).append("\"} ")
              .append(entry.getValue().get()).append("\n");
        }

        return sb.toString();
    }
}
