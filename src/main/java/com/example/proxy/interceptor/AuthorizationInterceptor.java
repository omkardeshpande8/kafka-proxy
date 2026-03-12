package com.example.proxy.interceptor;

import com.example.proxy.protocol.KafkaMessage;
import com.example.proxy.protocol.TopicExtractor;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.ssl.SslHandler;
import org.apache.kafka.common.protocol.ApiKeys;

import java.security.Principal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

public class AuthorizationInterceptor implements KafkaInterceptor {

    public enum Decision {
        ALLOW,
        DENY
    }

    static final class Rule {
        private final Decision decision;
        private final Pattern topicPattern;
        private final Pattern clientIdPattern;
        private final Pattern principalPattern;
        private final List<String> apiNames;

        Rule(Decision decision, Pattern topicPattern, Pattern clientIdPattern, Pattern principalPattern, List<String> apiNames) {
            this.decision = decision;
            this.topicPattern = topicPattern;
            this.clientIdPattern = clientIdPattern;
            this.principalPattern = principalPattern;
            this.apiNames = apiNames;
        }

        boolean matches(KafkaMessage message, String topic, String principalName) {
            if (topicPattern != null && (topic == null || !topicPattern.matcher(topic).matches())) {
                return false;
            }

            if (clientIdPattern != null && (message.clientId() == null || !clientIdPattern.matcher(message.clientId()).matches())) {
                return false;
            }

            if (principalPattern != null && (principalName == null || !principalPattern.matcher(principalName).matches())) {
                return false;
            }

            if (!apiNames.isEmpty()) {
                String apiName;
                try {
                    apiName = ApiKeys.forId(message.apiKey()).name.toUpperCase(Locale.ROOT);
                } catch (Exception ignored) {
                    apiName = "UNKNOWN";
                }

                if (!apiNames.contains(apiName)) {
                    return false;
                }
            }

            return true;
        }
    }

    private final Decision defaultDecision;
    private final List<Rule> rules;

    public AuthorizationInterceptor(Decision defaultDecision, List<Rule> rules) {
        this.defaultDecision = defaultDecision;
        this.rules = new ArrayList<>(rules);
    }

    @Override
    public void onRequest(ChannelHandlerContext ctx, KafkaMessage message, KafkaInterceptorChain.Callback callback) {
        String topic = TopicExtractor.extractTopic(message);
        String principal = resolvePrincipal(ctx);

        for (Rule rule : rules) {
            if (rule.matches(message, topic, principal)) {
                if (rule.decision == Decision.ALLOW) {
                    callback.proceed();
                } else {
                    callback.block();
                }
                return;
            }
        }

        if (defaultDecision == Decision.ALLOW) {
            callback.proceed();
        } else {
            callback.block();
        }
    }

    private String resolvePrincipal(ChannelHandlerContext ctx) {
        if (ctx == null) {
            return null;
        }

        SslHandler sslHandler = ctx.pipeline().get(SslHandler.class);
        if (sslHandler == null) {
            return null;
        }

        try {
            Principal peer = sslHandler.engine().getSession().getPeerPrincipal();
            return peer != null ? peer.getName() : null;
        } catch (Exception ignored) {
            return null;
        }
    }

    public static AuthorizationInterceptor fromConfig(String defaultAction, String rulesConfig) {
        Decision decision = "deny".equalsIgnoreCase(defaultAction) ? Decision.DENY : Decision.ALLOW;
        List<Rule> rules = parseRules(rulesConfig);
        return new AuthorizationInterceptor(decision, rules);
    }

    static List<Rule> parseRules(String rulesConfig) {
        if (rulesConfig == null || rulesConfig.trim().isEmpty()) {
            return Collections.emptyList();
        }

        List<Rule> rules = new ArrayList<>();
        for (String rawRule : rulesConfig.split(";")) {
            String trimmed = rawRule.trim();
            if (trimmed.isEmpty()) {
                continue;
            }

            int idx = trimmed.indexOf(':');
            if (idx <= 0) {
                continue;
            }

            String decisionToken = trimmed.substring(0, idx).trim();
            Decision decision = "deny".equalsIgnoreCase(decisionToken) ? Decision.DENY : Decision.ALLOW;

            Pattern topic = null;
            Pattern client = null;
            Pattern principal = null;
            List<String> apis = new ArrayList<>();

            String predicates = trimmed.substring(idx + 1);
            for (String fragment : predicates.split(",")) {
                String part = fragment.trim();
                if (part.isEmpty()) {
                    continue;
                }

                String[] kv = part.split("=", 2);
                if (kv.length != 2) {
                    continue;
                }

                String key = kv[0].trim().toLowerCase(Locale.ROOT);
                String value = kv[1].trim();

                if ("topic".equals(key)) {
                    topic = Pattern.compile(value);
                } else if ("client".equals(key) || "client_id".equals(key)) {
                    client = Pattern.compile(value);
                } else if ("principal".equals(key)) {
                    principal = Pattern.compile(value);
                } else if ("api".equals(key)) {
                    apis.add(value.toUpperCase(Locale.ROOT));
                }
            }

            rules.add(new Rule(decision, topic, client, principal, apis));
        }

        return rules;
    }
}
