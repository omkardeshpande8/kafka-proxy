package com.example.proxy;

import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;

public class TopicRoutingTest {

    @Test
    public void testTopicRouteResolution() {
        KafkaProxy proxy = new KafkaProxy(19092, "default-host", 19093);
        proxy.registerBackend("east", "east-host", 29093);
        proxy.registerBackend("west", "west-host", 39093);
        proxy.addOrUpdateTopicRoute("critical-.*", "east");
        proxy.addOrUpdateTopicRoute("analytics-.*", "west");

        KafkaProxy.BackendTarget criticalTarget = proxy.resolveBackend("critical-orders");
        KafkaProxy.BackendTarget analyticsTarget = proxy.resolveBackend("analytics-daily");
        KafkaProxy.BackendTarget fallbackTarget = proxy.resolveBackend("other-topic");

        assertEquals("east-host", criticalTarget.host());
        assertEquals(29093, criticalTarget.port());
        assertEquals("west-host", analyticsTarget.host());
        assertEquals(39093, analyticsTarget.port());
        assertEquals("default-host", fallbackTarget.host());
        assertEquals(19093, fallbackTarget.port());
    }

    @Test
    public void testApplyRoutingConfig() throws Exception {
        File config = File.createTempFile("proxy-routing", ".properties");
        config.deleteOnExit();

        String fileContent = "routing.backends=east,west\n"
                + "routing.backend.east.host=east-host\n"
                + "routing.backend.east.port=29093\n"
                + "routing.backend.west.host=west-host\n"
                + "routing.backend.west.port=39093\n"
                + "routing.default_backend=west\n"
                + "routing.topic_routes=critical-.*->east\n";

        try (FileOutputStream out = new FileOutputStream(config)) {
            out.write(fileContent.getBytes(StandardCharsets.UTF_8));
        }

        KafkaProxy proxy = new KafkaProxy(19092, "default-host", 19093);
        ProxyConfig.applyRoutingConfig(proxy, config.getAbsolutePath());

        KafkaProxy.BackendTarget criticalTarget = proxy.resolveBackend("critical-orders");
        KafkaProxy.BackendTarget fallbackTarget = proxy.resolveBackend("other-topic");

        assertEquals("east-host", criticalTarget.host());
        assertEquals(29093, criticalTarget.port());
        assertEquals("west-host", fallbackTarget.host());
        assertEquals(39093, fallbackTarget.port());
    }
}
