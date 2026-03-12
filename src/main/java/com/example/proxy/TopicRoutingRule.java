package com.example.proxy;

import java.util.regex.Pattern;

final class TopicRoutingRule {
    private final String rawPattern;
    private final Pattern topicPattern;
    private volatile String backendName;

    TopicRoutingRule(String rawPattern, String backendName) {
        this.rawPattern = rawPattern;
        this.topicPattern = Pattern.compile(rawPattern);
        this.backendName = backendName;
    }

    boolean matches(String topic) {
        return topicPattern.matcher(topic).matches();
    }

    String backendName() {
        return backendName;
    }

    String rawPattern() {
        return rawPattern;
    }

    void setBackendName(String backendName) {
        this.backendName = backendName;
    }
}
