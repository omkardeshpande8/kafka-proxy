package com.mycompany.proxy;

import com.mycompany.proxy.interceptor.*;
import com.mycompany.proxy.security.ProxySecurityConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.util.Properties;

public class ProxyConfig {
    private static final Logger logger = LoggerFactory.getLogger(ProxyConfig.class);

    public static KafkaInterceptorChain loadInterceptors(String configPath) {
        KafkaInterceptorChain chain = new KafkaInterceptorChain();
        Properties props = loadProperties(configPath);
        try {
            // Priority 1: Auth
            if (Boolean.parseBoolean(props.getProperty("interceptor.authz.enabled", "false"))) {
                String defaultAction = props.getProperty("interceptor.authz.default_action", "allow");
                String rules = props.getProperty("interceptor.authz.rules", "");
                chain.addInterceptor(AuthorizationInterceptor.fromConfig(defaultAction, rules));
                logger.info("Authorization Interceptor enabled (defaultAction={})", defaultAction);
            }

            // Priority 2: Guardrails
            String blockedTopics = props.getProperty("interceptor.guardrail.blocked_topics");
            if (blockedTopics != null && !blockedTopics.isEmpty()) {
                TopicGuardrailInterceptor guardrail = new TopicGuardrailInterceptor();
                for (String topic : blockedTopics.split(",")) {
                    guardrail.blockTopic(topic.trim());
                }
                chain.addInterceptor(guardrail);
                logger.info("Topic Guardrail enabled with topics: {}", blockedTopics);
            }

            // Other interceptors
            if (Boolean.parseBoolean(props.getProperty("interceptor.audit.enabled", "false"))) {
                chain.addInterceptor(new AuditInterceptor());
                logger.info("Audit Interceptor enabled");
            }

            String virtualTopic = props.getProperty("interceptor.alias.virtual");
            String physicalTopic = props.getProperty("interceptor.alias.physical");
            if (virtualTopic != null && physicalTopic != null) {
                TopicAliasInterceptor aliaser = new TopicAliasInterceptor();
                aliaser.addAlias(virtualTopic, physicalTopic);
                chain.addInterceptor(aliaser);
                logger.info("Topic Aliasing enabled: {} -> {}", virtualTopic, physicalTopic);
            }

            int chaosLatency = Integer.parseInt(props.getProperty("interceptor.chaos.latency", "0"));
            double chaosErrorRate = Double.parseDouble(props.getProperty("interceptor.chaos.error_rate", "0"));
            if (chaosLatency > 0 || chaosErrorRate > 0) {
                chain.addInterceptor(new ChaosInterceptor(chaosLatency, chaosErrorRate));
                logger.info("Chaos Interceptor enabled (latency={}ms, errorRate={})", chaosLatency, chaosErrorRate);
            }

            long rateLimitMaxBps = Long.parseLong(props.getProperty("interceptor.ratelimit.max_bps", "0"));
            if (rateLimitMaxBps > 0) {
                chain.addInterceptor(new RateLimitInterceptor(rateLimitMaxBps));
                logger.info("Rate Limit Interceptor enabled (maxBps={})", rateLimitMaxBps);
            }

            if (Boolean.parseBoolean(props.getProperty("interceptor.dataquality.json_validation", "false"))) {
                chain.addInterceptor(new JsonValidationInterceptor());
                logger.info("JSON Validation Interceptor enabled");
            }

            String maskingFields = props.getProperty("interceptor.masking.fields");
            if (maskingFields != null && !maskingFields.isEmpty()) {
                FieldMaskingInterceptor masker = new FieldMaskingInterceptor();
                for (String field : maskingFields.split(",")) {
                    masker.addMaskedField(field.trim());
                }
                chain.addInterceptor(masker);
                logger.info("Field Masking enabled for fields: {}", maskingFields);
            }

            int offloadThreshold = Integer.parseInt(props.getProperty("interceptor.offload.threshold_bytes", "0"));
            if (offloadThreshold > 0) {
                chain.addInterceptor(new OffloadInterceptor(offloadThreshold));
                logger.info("Payload Offloading enabled (threshold={} bytes)", offloadThreshold);
            }

            if (Boolean.parseBoolean(props.getProperty("interceptor.cache.enabled", "false"))) {
                chain.addInterceptor(new CacheInterceptor());
                logger.info("Fetch Caching enabled");
            }

            String sqlFilter = props.getProperty("interceptor.sql.filter");
            if (sqlFilter != null && !sqlFilter.isEmpty()) {
                chain.addInterceptor(new VirtualSqlInterceptor(sqlFilter));
                logger.info("Virtual SQL Filter enabled: {}", sqlFilter);
            }

        } catch (Exception e) {
            logger.error("Could not parse interceptor config from {}, using defaults. {}", configPath, e.getMessage());
        }
        return chain;
    }

    public static ProxySecurityConfig loadSecurityConfig(String configPath) {
        Properties props = loadProperties(configPath);
        return ProxySecurityConfig.load(props);
    }

    public static void applyRoutingConfig(KafkaProxy proxy, String configPath) {
        Properties props = loadProperties(configPath);

        String backends = props.getProperty("routing.backends", "").trim();
        if (!backends.isEmpty()) {
            for (String backendName : backends.split(",")) {
                String name = backendName.trim();
                if (name.isEmpty()) {
                    continue;
                }

                String host = props.getProperty("routing.backend." + name + ".host");
                String port = props.getProperty("routing.backend." + name + ".port");
                if (host == null || port == null) {
                    System.err.println("[ROUTING] Missing host/port for backend " + name + ", skipping");
                    continue;
                }

                proxy.registerBackend(name, host.trim(), Integer.parseInt(port.trim()));
                logger.info("[ROUTING] Registered backend {} -> {}:{}", name, host, port);
            }
        }

        String defaultBackend = props.getProperty("routing.default_backend", "").trim();
        if (!defaultBackend.isEmpty()) {
            proxy.setDefaultBackend(defaultBackend);
            logger.info("[ROUTING] Default backend set to {}", defaultBackend);
        }

        String routes = props.getProperty("routing.topic_routes", "").trim();
        if (!routes.isEmpty()) {
            for (String route : routes.split(",")) {
                String definition = route.trim();
                if (definition.isEmpty()) {
                    continue;
                }

                String[] parts = definition.split("->");
                if (parts.length != 2) {
                    System.err.println("[ROUTING] Invalid route definition: " + definition);
                    continue;
                }

                proxy.addOrUpdateTopicRoute(parts[0].trim(), parts[1].trim());
            }
        }
    }

    private static Properties loadProperties(String configPath) {
        Properties props = new Properties();
        try (FileInputStream in = new FileInputStream(configPath)) {
            props.load(in);
        } catch (Exception e) {
            System.err.println("Could not load config from " + configPath + ", using defaults. " + e.getMessage());
        }
        return props;
    }
}
