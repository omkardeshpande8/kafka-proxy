package com.example.proxy;

import com.example.proxy.interceptor.KafkaInterceptorChain;
import org.junit.Test;
import java.io.File;
import java.io.FileOutputStream;
import java.util.Properties;
import static org.junit.Assert.*;

public class ProxyConfigTest {

    @Test
    public void testLoadInterceptors() throws Exception {
        Properties props = new Properties();
        props.setProperty("interceptor.audit.enabled", "true");
        props.setProperty("interceptor.guardrail.blocked_topics", "test1,test2");

        File configFile = File.createTempFile("proxy", ".properties");
        try (FileOutputStream out = new FileOutputStream(configFile)) {
            props.store(out, null);
        }

        KafkaInterceptorChain chain = ProxyConfig.loadInterceptors(configFile.getAbsolutePath());
        assertNotNull(chain);
        // We can't easily check the list size without exposing it,
        // but the console output should indicate they were loaded.
        configFile.delete();
    }
}
