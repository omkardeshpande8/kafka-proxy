package com.mycompany.proxy;

import com.mycompany.proxy.KafkaProxy;
import org.junit.Test;
import java.io.File;
import java.io.FileOutputStream;
import java.util.Properties;
import static org.junit.Assert.*;

public class TopicRoutingTest {

    @Test
    public void testTopicRoutingResolution() {
        KafkaProxy proxy = new KafkaProxy(19092, "default-host", 19093);
        proxy.registerBackend("east", "east-host", 29093);
        proxy.registerBackend("west", "west-host", 39093);

        proxy.addOrUpdateTopicRoute("critical-.*", "east");
        proxy.addOrUpdateTopicRoute("analytics-.*", "west");

        KafkaProxy.BackendTarget criticalTarget = proxy.resolveBackend("critical-orders", null);
        KafkaProxy.BackendTarget analyticsTarget = proxy.resolveBackend("analytics-daily", null);
        KafkaProxy.BackendTarget fallbackTarget = proxy.resolveBackend("other-topic", null);

        assertEquals("east-host", criticalTarget.host());
        assertEquals(29093, criticalTarget.port());

        assertEquals("west-host", analyticsTarget.host());
        assertEquals(39093, analyticsTarget.port());

        assertEquals("default-host", fallbackTarget.host());
        assertEquals(19093, fallbackTarget.port());
    }

    @Test
    public void testRoutingFromConfig() throws Exception {
        File config = File.createTempFile("proxy", ".properties");
        try (FileOutputStream out = new FileOutputStream(config)) {
            Properties props = new Properties();
            props.setProperty("routing.backends", "east,west");
            props.setProperty("routing.backend.east.host", "east-host");
            props.setProperty("routing.backend.east.port", "29093");
            props.setProperty("routing.backend.west.host", "west-host");
            props.setProperty("routing.backend.west.port", "39093");
            props.setProperty("routing.topic_routes", "critical-.* -> east");
            props.store(out, null);
        }

        KafkaProxy proxy = new KafkaProxy(19092, "default-host", 19093);
        ProxyConfig.applyRoutingConfig(proxy, config.getAbsolutePath());

        KafkaProxy.BackendTarget criticalTarget = proxy.resolveBackend("critical-orders", null);
        KafkaProxy.BackendTarget fallbackTarget = proxy.resolveBackend("other-topic", null);

        assertEquals("east-host", criticalTarget.host());
        assertEquals(29093, criticalTarget.port());

        assertEquals("default-host", fallbackTarget.host());
    }
}
