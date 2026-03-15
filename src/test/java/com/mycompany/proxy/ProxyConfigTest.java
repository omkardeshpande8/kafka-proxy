package com.mycompany.proxy;

import com.mycompany.proxy.interceptor.KafkaInterceptorChain;
import org.junit.Test;
import java.io.File;
import java.io.FileOutputStream;
import java.util.Properties;
import static org.junit.Assert.*;

public class ProxyConfigTest {

    @Test
    public void testLoadAllInterceptors() throws Exception {
        Properties props = new Properties();
        props.setProperty("interceptor.audit.enabled", "true");
        props.setProperty("interceptor.guardrail.blocked_topics", "test1,test2");
        props.setProperty("interceptor.alias.virtual", "v1");
        props.setProperty("interceptor.alias.physical", "p1");
        props.setProperty("interceptor.chaos.latency", "100");
        props.setProperty("interceptor.chaos.error_rate", "0.01");
        props.setProperty("interceptor.ratelimit.max_bps", "1000");
        props.setProperty("interceptor.dataquality.json_validation", "true");
        props.setProperty("interceptor.masking.fields", "f1,f2");
        props.setProperty("interceptor.offload.threshold_bytes", "500");
        props.setProperty("interceptor.cache.enabled", "true");

        File configFile = File.createTempFile("proxy", ".properties");
        try (FileOutputStream out = new FileOutputStream(configFile)) {
            props.store(out, null);
        }

        KafkaInterceptorChain chain = ProxyConfig.loadInterceptors(configFile.getAbsolutePath());
        assertNotNull(chain);
        configFile.delete();
    }
}
