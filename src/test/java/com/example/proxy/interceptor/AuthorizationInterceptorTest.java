package com.example.proxy.interceptor;

import com.example.proxy.protocol.KafkaMessage;
import io.netty.buffer.Unpooled;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AuthorizationInterceptorTest {

    @Test
    public void parsesRulesFromConfigString() {
        List<AuthorizationInterceptor.Rule> rules = AuthorizationInterceptor.parseRules(
                "allow:api=PRODUCE,client=payments-.*;deny:topic=secret-.*"
        );

        assertEquals(2, rules.size());
    }

    @Test
    public void defaultDenyBlocksWhenNoRuleMatches() {
        AuthorizationInterceptor interceptor = AuthorizationInterceptor.fromConfig("deny", "allow:api=FETCH");
        KafkaMessage message = new KafkaMessage(Unpooled.buffer(0), (short) 0, (short) 2, 1, "client-a", 0);

        AtomicBoolean proceeded = new AtomicBoolean(false);
        AtomicBoolean blocked = new AtomicBoolean(false);

        interceptor.onRequest(null, message, new KafkaInterceptorChain.Callback() {
            @Override
            public void proceed() {
                proceeded.set(true);
            }

            @Override
            public void block() {
                blocked.set(true);
            }
        });

        assertFalse(proceeded.get());
        assertTrue(blocked.get());
        message.release();
    }

    @Test
    public void allowsWhenApiAndClientRuleMatches() {
        AuthorizationInterceptor interceptor = AuthorizationInterceptor.fromConfig(
                "deny",
                "allow:api=PRODUCE,client=orders-.*"
        );

        KafkaMessage message = new KafkaMessage(Unpooled.buffer(0), (short) 0, (short) 2, 2, "orders-service", 0);

        AtomicBoolean proceeded = new AtomicBoolean(false);
        AtomicBoolean blocked = new AtomicBoolean(false);

        interceptor.onRequest(null, message, new KafkaInterceptorChain.Callback() {
            @Override
            public void proceed() {
                proceeded.set(true);
            }

            @Override
            public void block() {
                blocked.set(true);
            }
        });

        assertTrue(proceeded.get());
        assertFalse(blocked.get());
        message.release();
    }

    @Test
    public void parsesPrincipalWithCommas() {
        List<AuthorizationInterceptor.Rule> rules = AuthorizationInterceptor.parseRules(
                "allow:principal=CN=svc,OU=team,O=org,api=PRODUCE"
        );

        assertEquals(1, rules.size());
        
        KafkaMessage message = new KafkaMessage(Unpooled.buffer(0), (short) 0, (short) 2, 0, "client", 0);
        assertTrue(rules.get(0).matches(message, "topic", "CN=svc,OU=team,O=org"));
        assertFalse(rules.get(0).matches(message, "topic", "CN=other"));
        message.release();
    }
}
