package com.example.proxy;

import com.example.proxy.interceptor.*;

import java.io.FileInputStream;
import java.util.Properties;

public class ProxyConfig {
    public static KafkaInterceptorChain loadInterceptors(String configPath) {
        KafkaInterceptorChain chain = new KafkaInterceptorChain();
        Properties props = new Properties();
        try (FileInputStream in = new FileInputStream(configPath)) {
            props.load(in);

            if (Boolean.parseBoolean(props.getProperty("interceptor.audit.enabled", "false"))) {
                chain.addInterceptor(new AuditInterceptor());
                System.out.println("Audit Interceptor enabled");
            }

            String blockedTopics = props.getProperty("interceptor.guardrail.blocked_topics");
            if (blockedTopics != null && !blockedTopics.isEmpty()) {
                TopicGuardrailInterceptor guardrail = new TopicGuardrailInterceptor();
                for (String topic : blockedTopics.split(",")) {
                    guardrail.blockTopic(topic.trim());
                }
                chain.addInterceptor(guardrail);
                System.out.println("Topic Guardrail enabled with topics: " + blockedTopics);
            }

            String virtualTopic = props.getProperty("interceptor.alias.virtual");
            String physicalTopic = props.getProperty("interceptor.alias.physical");
            if (virtualTopic != null && physicalTopic != null) {
                TopicAliasInterceptor aliaser = new TopicAliasInterceptor();
                aliaser.addAlias(virtualTopic, physicalTopic);
                chain.addInterceptor(aliaser);
                System.out.println("Topic Aliasing enabled: " + virtualTopic + " -> " + physicalTopic);
            }

            int chaosLatency = Integer.parseInt(props.getProperty("interceptor.chaos.latency", "0"));
            double chaosErrorRate = Double.parseDouble(props.getProperty("interceptor.chaos.error_rate", "0"));
            if (chaosLatency > 0 || chaosErrorRate > 0) {
                chain.addInterceptor(new ChaosInterceptor(chaosLatency, chaosErrorRate));
                System.out.println("Chaos Interceptor enabled (latency=" + chaosLatency + "ms, errorRate=" + chaosErrorRate + ")");
            }

            long rateLimitMaxBps = Long.parseLong(props.getProperty("interceptor.ratelimit.max_bps", "0"));
            if (rateLimitMaxBps > 0) {
                chain.addInterceptor(new RateLimitInterceptor(rateLimitMaxBps));
                System.out.println("Rate Limit Interceptor enabled (maxBps=" + rateLimitMaxBps + ")");
            }

            if (Boolean.parseBoolean(props.getProperty("interceptor.dataquality.json_validation", "false"))) {
                chain.addInterceptor(new JsonValidationInterceptor());
                System.out.println("JSON Validation Interceptor enabled");
            }

            String maskingFields = props.getProperty("interceptor.masking.fields");
            if (maskingFields != null && !maskingFields.isEmpty()) {
                FieldMaskingInterceptor masker = new FieldMaskingInterceptor();
                for (String field : maskingFields.split(",")) {
                    masker.addMaskedField(field.trim());
                }
                chain.addInterceptor(masker);
                System.out.println("Field Masking enabled for fields: " + maskingFields);
            }

            int offloadThreshold = Integer.parseInt(props.getProperty("interceptor.offload.threshold_bytes", "0"));
            if (offloadThreshold > 0) {
                chain.addInterceptor(new OffloadInterceptor(offloadThreshold));
                System.out.println("Payload Offloading enabled (threshold=" + offloadThreshold + " bytes)");
            }

            if (Boolean.parseBoolean(props.getProperty("interceptor.cache.enabled", "false"))) {
                chain.addInterceptor(new CacheInterceptor());
                System.out.println("Fetch Caching enabled");
            }

            String sqlFilter = props.getProperty("interceptor.sql.filter");
            if (sqlFilter != null && !sqlFilter.isEmpty()) {
                chain.addInterceptor(new VirtualSqlInterceptor(sqlFilter));
                System.out.println("Virtual SQL Filter enabled: " + sqlFilter);
            }

        } catch (Exception e) {
            System.err.println("Could not load config from " + configPath + ", using defaults. " + e.getMessage());
        }
        return chain;
    }
}
