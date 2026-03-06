package com.example.proxy;

import com.example.proxy.interceptor.AuditInterceptor;
import com.example.proxy.interceptor.KafkaInterceptorChain;
import com.example.proxy.interceptor.TopicGuardrailInterceptor;

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

        } catch (Exception e) {
            System.err.println("Could not load config from " + configPath + ", using defaults. " + e.getMessage());
        }
        return chain;
    }
}
